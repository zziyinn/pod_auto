#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, argparse, tempfile
from datetime import datetime, timezone
from dateutil import parser as dtparser
from dateutil import tz
import pandas as pd
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

RESULT_MAP = {
    'Qualified': 0,
    'No Address Info': 1,
    'Location Not Clear': 2,
    'No Clear Shipping Label': 3,
    'Public or Unsafe Area': 4,
    'Invalid Mailbox Delivery': 5,
    'Leave Outside of Building': 6,
    'Wrong Address': 7,
    'Wrong Parcel Photo': 8,
    'No POD': 9,
    'Inappropriate Delivery': 10
}

def auth_sa(credentials_path: str) -> GoogleDrive:
    gauth = GoogleAuth(settings={
        "client_config_backend": "service",
        "service_config": {"client_json_file_path": credentials_path},
    })
    gauth.ServiceAuth()
    return GoogleDrive(gauth)

def list_children(drive: GoogleDrive, folder_id: str):
    return drive.ListFile({'q': f"'{folder_id}' in parents and trashed=false"}).GetList()

def subfolder_id(drive: GoogleDrive, parent_id: str, name: str) -> str:
    q = f"'{parent_id}' in parents and trashed=false and mimeType='application/vnd.google-apps.folder' and title='{name}'"
    res = drive.ListFile({'q': q}).GetList()
    if res: return res[0]['id']
    f = drive.CreateFile({'title': name, 'mimeType': 'application/vnd.google-apps.folder', 'parents': [{'id': parent_id}]})
    f.Upload()
    return f['id']

def find_by_name(drive: GoogleDrive, parent_id: str, name: str):
    res = drive.ListFile({'q': f"'{parent_id}' in parents and trashed=false and title='{name}'"}).GetList()
    return res[0] if res else None

def dl(drive: GoogleDrive, fobj, local_path: str): fobj.GetContentFile(local_path)

def upsert(drive: GoogleDrive, parent_id: str, local_path: str, name: str):
    ex = find_by_name(drive, parent_id, name)
    f = drive.CreateFile({'id': ex['id']}) if ex else drive.CreateFile({'title': name, 'parents': [{'id': parent_id}]})
    f.SetContentFile(local_path); f.Upload(); return f['id']

def parse_time(s: str):
    try: return dtparser.isoparse(s) if s else None
    except: return None

def is_today(dt, tzname: str) -> bool:
    if dt is None: return False
    tzinfo = tz.gettz(tzname) or tz.UTC
    return datetime.now(tzinfo).date() == dt.astimezone(tzinfo).date()

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in ['partner_id','team_id']:
        if col in df.columns: df[col] = df[col].astype(str).str.replace('.0','',regex=False)
    if 'zipcode' in df.columns: df['zipcode'] = df['zipcode'].astype(str).str.split('-').str[0]
    if 'VALID POD' in df.columns: df['VALID POD_encoded'] = df['VALID POD'].map({'Y':0,'N':1})
    if 'result' in df.columns: df['result_encoded'] = df['result'].map(RESULT_MAP).astype('Int64').astype(str)
    return df

def append_log_row(log_path: str, row: list):
    cols = ['timestamp','src_id','src_title','src_modified','dst_id','dst_title','rows_in','rows_out','status','message']
    if not os.path.exists(log_path): pd.DataFrame(columns=cols).to_csv(log_path, index=False)
    df = pd.read_csv(log_path) if os.path.exists(log_path) else pd.DataFrame(columns=cols)
    df = pd.concat([df, pd.Series(row, index=cols).to_frame().T], ignore_index=True)
    df.to_csv(log_path, index=False)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--folder-id', required=True)
    ap.add_argument('--credentials', default='credentials.json')
    ap.add_argument('--run-today-only', default='true')
    ap.add_argument('--tz', default='America/New_York')
    args = ap.parse_args()

    run_today_only = str(args.run_today_only).lower() == 'true'
    tzname = args.tz or 'America/New_York'

    drive = auth_sa(args.credentials)
    root_id = args.folder_id
    clean_id = subfolder_id(drive, root_id, 'data_cleaned')
    LOG_NAME = '_pipeline_log.csv'

    with tempfile.TemporaryDirectory() as td:
        # 拉取旧日志（如有）
        local_log = os.path.join(td, LOG_NAME)
        exlog = find_by_name(drive, clean_id, LOG_NAME)
        if exlog: dl(drive, exlog, local_log)

        files = [f for f in list_children(drive, root_id) if f.get('title','').lower().endswith('.csv')]
        processed = skipped = failed = 0

        for f in files:
            fid, title = f['id'], f['title']
            src_mdt = parse_time(f.get('modifiedDate'))
            if run_today_only and not is_today(src_mdt, tzname): continue

            base = os.path.splitext(title)[0]
            cleaned_name = f"{base}_cleaned.csv"
            ex_clean = find_by_name(drive, clean_id, cleaned_name)
            if ex_clean:
                clean_mdt = parse_time(ex_clean.get('modifiedDate'))
                if src_mdt and clean_mdt and clean_mdt >= src_mdt:
                    skipped += 1; continue

            try:
                local_src = os.path.join(td, title); dl(drive, f, local_src)
                # 读入（简单容错）
                read_ok, df_raw = False, None
                for enc in [None, 'utf-8', 'utf-8-sig', 'latin1']:
                    try: df_raw = pd.read_csv(local_src, encoding=enc); read_ok = True; break
                    except: pass
                if not read_ok: raise RuntimeError('无法读取 CSV（编码不兼容）')

                rows_in = len(df_raw)
                df_clean = clean_dataframe(df_raw); rows_out = len(df_clean)
                local_clean = os.path.join(td, cleaned_name); df_clean.to_csv(local_clean, index=False)
                dst_id = upsert(drive, clean_id, local_clean, cleaned_name)

                append_log_row(local_log, [
                    datetime.utcnow().replace(tzinfo=timezone.utc).isoformat(timespec='seconds'),
                    fid, title, f.get('modifiedDate'), dst_id, cleaned_name, rows_in, rows_out, 'ok', ''
                ])
                processed += 1
                print(f"OK: {title} -> {cleaned_name} ({rows_in}→{rows_out})")
            except Exception as e:
                append_log_row(local_log, [
                    datetime.utcnow().replace(tzinfo=timezone.utc).isoformat(timespec='seconds'),
                    fid, title, f.get('modifiedDate'), '', cleaned_name, '', '', 'fail', str(e)
                ])
                failed += 1
                print(f"FAIL: {title} | {e}")

        upsert(drive, clean_id, local_log, LOG_NAME)
        print(f"\nSummary → processed: {processed}, skipped: {skipped}, failed: {failed}")
        print("Log saved: data_cleaned/_pipeline_log.csv")

if __name__ == '__main__':
    main()
