import os, json, io, zipfile, time
import pandas as pd
import gspread
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# --- CONFIGURATION ---
FOLDER_ID = os.environ['FOLDER_ID']
SHEET_ID = os.environ['SHEET_ID']
# We do NOT need SEATALK_URL here anymore; Apps Script handles it.
SERVICE_ACCOUNT_INFO = json.loads(os.environ['GOOGLE_SERVICE_ACCOUNT_JSON'])

# --- AUTHENTICATION ---
scopes = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
creds = service_account.Credentials.from_service_account_info(SERVICE_ACCOUNT_INFO, scopes=scopes)
drive_service = build('drive', 'v3', credentials=creds)
gc = gspread.authorize(creds)

def main():
    print("--- STARTING AUTOMATION ---")
    
    # 1. CONNECT TO SHEET & CLEAR HANDSHAKE CELL
    sh = gc.open_by_key(SHEET_ID)
    dashboard_sheet = sh.worksheet("Backlogs Summary")
    # Clear the signal cell first so no false triggers happen
    dashboard_sheet.update('A1', [['']]) 
    print("Handshake cell (A1) cleared.")

    # 2. FIND NEWEST ZIP
    results = drive_service.files().list(
        q=f"'{FOLDER_ID}' in parents and name contains '.zip'",
        fields="files(id, name)", orderBy="createdTime desc", pageSize=1
    ).execute()
    
    if not results.get('files'):
        print("No ZIP file found.")
        return

    zip_file = results['files'][0]
    print(f"Downloading: {zip_file['name']}")

    # 3. DOWNLOAD & EXTRACT
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

    if not all_dfs:
        print("No CSVs found in ZIP.")
        return
        
    df = pd.concat(all_dfs, ignore_index=True)

    # 4. FILTERING LOGIC
    # Clean headers
    df.columns = df.columns.str.strip()
    
    # Apply Filters (Station + SOC 5)
    mask = (df['Receiver type'].astype(str).str.lower() == 'station') & \
           (df['Current Station'].astype(str).str.lower() == 'soc 5')
    
    cols = ['TO Number', 'SPX Tracking Number', 'Receiver Name', 'TO Order Quantity', 
            'Operator', 'Create Time', 'Complete Time', 'Remark', 'Receive Status', 'Staging Area ID']
    
    # Fill empty cells to prevent JSON errors
    filtered_df = df[mask][cols].fillna('')
    total_rows = len(filtered_df)
    print(f"Data processed. Rows to upload: {total_rows}")

    # 5. UPLOAD TO DATA SHEET (IN CHUNKS)
    # REPLACE "RawData" with the actual name of your destination tab
    DESTINATION_TAB_NAME = "RawData" 
    
    try:
        data_sheet = sh.worksheet(DESTINATION_TAB_NAME)
    except gspread.exceptions.WorksheetNotFound:
        print(f"Error: Could not find tab named '{DESTINATION_TAB_NAME}'. Check your Sheet!")
        return

    data_sheet.clear()
    
    # Upload Headers
    data_sheet.update('A1', [filtered_df.columns.values.tolist()])
    
    # Upload Rows in 10k Chunks
    chunk_size = 10000
    for i in range(0, total_rows, chunk_size):
        chunk = filtered_df.iloc[i:i + chunk_size].values.tolist()
        start_row = i + 2
        data_sheet.update(f'A{start_row}', chunk)
        print(f"Uploaded rows {i} to {i + len(chunk)}")

    # 6. THE HANDSHAKE (FINAL STEP)
    # Only write this when EVERYTHING is done.
    # This triggers the Google Apps Script.
    print("Writing completion signal...")
    dashboard_sheet.update('A1', [['COMPLETED']])
    print("SUCCESS: Automation Finished.")

if __name__ == "__main__":
    main()
