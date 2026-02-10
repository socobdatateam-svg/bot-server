import os, json, io, zipfile, requests
import pandas as pd
import gspread
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# 1. SETUP
FOLDER_ID = os.environ['FOLDER_ID']
SHEET_ID = os.environ['SHEET_ID']
SEATALK_URL = os.environ['SEATALK_WEBHOOK']
SERVICE_ACCOUNT_INFO = json.loads(os.environ['GOOGLE_SERVICE_ACCOUNT_JSON'])

# 2. AUTH
scopes = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
creds = service_account.Credentials.from_service_account_info(SERVICE_ACCOUNT_INFO, scopes=scopes)
drive_service = build('drive', 'v3', credentials=creds)
gc = gspread.authorize(creds)

def main():
    # 3. GET NEWEST ZIP
    results = drive_service.files().list(
        q=f"'{FOLDER_ID}' in parents and name contains '.zip'",
        fields="files(id, name)", orderBy="createdTime desc", pageSize=1
    ).execute()
    
    if not results.get('files'):
        print("No ZIP found.")
        return
    
    zip_file = results['files'][0]
    print(f"Processing: {zip_file['name']}")

    # 4. DOWNLOAD & MERGE
    request = drive_service.files().get_media(fileId=zip_file['id'])
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done: _, done = downloader.next_chunk()

    all_dfs = []
    with zipfile.ZipFile(fh) as z:
        for filename in z.namelist():
            if filename.lower().endswith('.csv'):
                with z.open(filename) as f:
                    all_dfs.append(pd.read_csv(f, encoding='utf-8-sig'))

    if not all_dfs: return
    df = pd.concat(all_dfs, ignore_index=True)

    # 5. FILTERING & CLEANING (The Fix)
    df.columns = df.columns.str.strip()
    
    # Filtering based on your workflow logic
    mask = (df['Receiver type'].astype(str).str.lower() == 'station') & \
           (df['Current Station'].astype(str).str.lower() == 'soc 5')
    
    cols = ['TO Number', 'SPX Tracking Number', 'Receiver Name', 'TO Order Quantity', 
            'Operator', 'Create Time', 'Complete Time', 'Remark', 'Receive Status', 'Staging Area ID']
    
    filtered_df = df[mask][cols]

    # THE CRITICAL FIX: Convert "NaN" (Not a Number) to empty strings
    filtered_df = filtered_df.fillna('')

    # 6. UPDATE SHEET
    sh = gc.open_by_key(SHEET_ID)
    worksheet = sh.get_worksheet(0)
    
    # Prepare data for upload (headers + rows)
    data_to_upload = [filtered_df.columns.values.tolist()] + filtered_df.values.tolist()
    
    worksheet.clear()
    worksheet.update('A1', data_to_upload)
    print(f"Success! Uploaded {len(filtered_df)} rows.")

    # 7. SEATALK NOTIFICATION
    msg = {
        "tag": "text",
        "text": {
            "content": f"✅ Dashboard Updated!\nFile: {zip_file['name']}\nFiltered Rows: {len(filtered_df)}",
            "at_all": False
        }
    }
    requests.post(SEATALK_URL, json=msg)

if __name__ == "__main__":
    main()
