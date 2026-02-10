import os, json, io, zipfile, requests
import pandas as pd
import gspread
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# 1. SETUP CONFIGURATION
FOLDER_ID = os.environ['FOLDER_ID']
SHEET_ID = os.environ['SHEET_ID']
SEATALK_URL = os.environ['SEATALK_WEBHOOK']
SERVICE_ACCOUNT_INFO = json.loads(os.environ['GOOGLE_SERVICE_ACCOUNT_JSON'])

# 2. AUTHENTICATION
scopes = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
creds = service_account.Credentials.from_service_account_info(SERVICE_ACCOUNT_INFO, scopes=scopes)
drive_service = build('drive', 'v3', credentials=creds)
gc = gspread.authorize(creds)

def main():
    # 3. FIND NEWEST ZIP
    results = drive_service.files().list(
        q=f"'{FOLDER_ID}' in parents and name contains '.zip'",
        fields="files(id, name)",
        orderBy="createdTime desc",
        pageSize=1
    ).execute()
    
    files = results.get('files', [])
    if not files:
        print("No ZIP file found.")
        return

    zip_file = files[0]
    print(f"Processing: {zip_file['name']}")

    # 4. DOWNLOAD & EXTRACT CSVs
    request = drive_service.files().get_media(fileId=zip_file['id'])
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()

    all_dfs = []
    with zipfile.ZipFile(fh) as z:
        for filename in z.namelist():
            if filename.endswith('.csv'):
                with z.open(filename) as f:
                    all_dfs.append(pd.read_csv(f))

    # 5. MERGE & FILTER (Logic based on your workflow)
    df = pd.concat(all_dfs, ignore_index=True)
    
    # Example Filter: Only keep rows where 'Status' is 'Success' 
    # (Adjust this part based on your specific CSV columns)
    if 'Status' in df.columns:
        df = df[df['Status'] == 'Success']

    # 6. UPDATE GOOGLE SHEET
    sh = gc.open_by_key(SHEET_ID)
    worksheet = sh.get_worksheet(0) # First tab
    worksheet.clear()
    worksheet.update([df.columns.values.tolist()] + df.values.tolist())
    print("Google Sheet Updated!")

    # 7. NOTIFY SEATALK
    message = {
        "tag": "text",
        "text": {
            "content": f"✅ Automation Success!\nFile: {zip_file['name']}\nRows Processed: {len(df)}"
        }
    }
    requests.post(SEATALK_URL, json=message)

if __name__ == "__main__":
    main()
