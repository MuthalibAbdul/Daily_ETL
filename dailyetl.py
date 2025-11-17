import os
import pandas as pd
import gspread
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
import time
from tqdm import tqdm
import io
import pandas as pd
from googleapiclient.http import MediaIoBaseDownload
import pandas as pd
from sqlalchemy import create_engine
import logging
from dotenv import load_dotenv
import os
 
# Load the .env file
load_dotenv()
 
# Access variables
server = os.getenv("server")
database = os.getenv("database")
username = os.getenv("username")
password = os.getenv("password")

# --- Setup logger ---
# custom converter to shift time by -5 hours
# Custom time converter (example: shift timezone by -5 hours)
import time

def custom_time(*args):
    return time.localtime(time.mktime(time.localtime()) - 5 * 3600)
import logging

# Common formatter with custom time
formatter = logging.Formatter(
"%(asctime)s - %(levelname)s - %(message)s",
datefmt="%Y-%m-%d %H:%M:%S"
)
formatter.converter = custom_time

# --- Insert logger ---
insert_logger = logging.getLogger("insert_logger")
insert_logger.setLevel(logging.INFO)

insert_handler = logging.FileHandler("insert_log.log")
insert_handler.setFormatter(formatter)
insert_logger.addHandler(insert_handler)

# --- Not Find logger ---
notfind_logger = logging.getLogger("notfind_logger")
notfind_logger.setLevel(logging.INFO)

notfind_handler = logging.FileHandler("not_find_log.log")
notfind_handler.setFormatter(formatter)
notfind_logger.addHandler(notfind_handler)

# --- Usage ---
SCOPES = [
'https://www.googleapis.com/auth/drive',
'https://www.googleapis.com/auth/spreadsheets'
]

CLIENT_SECRET_FILE = 'client_secret.json'
TOKEN_FILE = 'token.json'

creds = None
if os.path.exists(TOKEN_FILE):
    creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

# Refresh or initiate new flow
if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
        creds = flow.run_local_server(port=0)  # This will open browser login
# Save the token for future use
    with open(TOKEN_FILE, 'w') as token:
        token.write(creds.to_json())

# ✅ Use credentials to initialize clients
gc = gspread.authorize(creds)
drive_service = build('drive', 'v3', credentials=creds)


# Replace with your folder ID
def get_sheets_in_folder(folder_id):
    query = f"'{folder_id}' in parents and mimeType='application/vnd.google-apps.spreadsheet'"
    result = drive_service.files().list(q=query, fields="files(id, name)").execute()
    return result.get('files', [])



# TARGET_HEADER = "Intervention Reason"
# def get_sheet_data(xl, sheet_name):
#     df_raw = xl.parse(sheet_name, header=None)
#     for i in range(len(df_raw)):
#         if TARGET_HEADER in df_raw.iloc[i].astype(str).values:
#             header_row = i
#             df_data = df_raw.iloc[header_row + 1:].copy()
#             df_data.columns = df_raw.iloc[header_row]
#             df_data = df_data.reset_index(drop=True)
#             return df_data

#     # If header not found
#     return pd.DataFrame()


TARGET_HEADER = "Id"
def get_sheet_data_Intervention_Reason(xl, sheet_name):
    df_raw = xl.parse(sheet_name, header=None)
    df_data = pd.DataFrame()  # Initialize as empty DataFrame
    
    for i in range(len(df_raw)):
        if TARGET_HEADER in df_raw.iloc[i].astype(str).values:
            header_row = i
            df_data = df_raw.iloc[header_row + 1:].copy()
            df_data.columns = df_raw.iloc[header_row]
            df_data = df_data.reset_index(drop=True)
            break  # Exit loop once header is found

    # If header was found and dataframe was created
    if not df_data.empty and 'Patient Name (Last, First)' in df_data.columns and TARGET_HEADER in df_data.columns:
        # print(df_data.head())
        # print(df_data.columns)
        # Create a mask: True for rows where Patient Name is empty/NaN AND Intervention Reason is not empty/NaN
        df_data = df_data.loc[:, ~df_data.columns.duplicated(keep='first')]
        df_data = df_data[
    df_data['Patient Name (Last, First)'].notna() &
    (df_data['Patient Name (Last, First)'] != '') &
    df_data['Id'].notna() &
    (df_data['Id'] != '')
]

    return df_data

# If header not found
    return pd.DataFrame()

def get_sheet_data_with_position(xl, sheet_name):
    df_raw = xl.parse(sheet_name, header=None)

    header_row = 16
    df_data = df_raw.iloc[header_row + 1:].copy()
    df_data.columns = df_raw.iloc[header_row]
    df_data = df_data.reset_index(drop=True)

    # --- NEW CONDITION: Filter out rows where Patient Name is empty BUT Intervention Reason has data ---
    # Check if the required columns exist in the dataframe
    if 'Patient Name (Last, First)' in df_data.columns and TARGET_HEADER in df_data.columns:
        # Create a mask: True for rows where Patient Name is empty/NaN AND Intervention Reason is not empty/NaN
        print(df_data.head())
        print(df.columns)
        mask_to_remove = (
        df_data['Patient Name (Last, First)'].isna() | 
        (df_data['Patient Name (Last, First)'].astype(str).str.strip() == '')
        ) & (
        df_data[TARGET_HEADER].notna() & 
        (df_data[TARGET_HEADER].astype(str).str.strip() != '')
        )

        # Keep rows that do NOT match the condition (i.e., remove the problematic rows)
        df_data = df_data[~mask_to_remove]

        return df_data

    # If header not found
    return pd.DataFrame()

def download_google_sheet_to_memory(file_id, drive_service):
    request = drive_service.files().export_media(
    fileId=file_id,
    mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )

    file_buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(file_buffer, request)

    done = False
    while not done:
        status, done = downloader.next_chunk()

    file_buffer.seek(0)  # Reset pointer to start for reading
    return file_buffer

def preprocess(df):
    df = df.rename(columns={
    "Intervention Reason": "Intervention_Reason",
    "Billing Date": "Billing_Date",
    "Billed Yes/No": "Billed",
    "SNF/Telehealth": "Mode",
    "Date of Service": "Date_of_Service",
    "Note Posted": "Note_Posted",
    "Patient Name (Last, First)": "Patient_Name",
    "CPT Code": "CPT_Code"
    })

    # Convert date-like columns
    for col in ["DOB", "Date_of_Service", "Billing_Date"]:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
        df[col] = pd.to_datetime(df[col]).dt.strftime("%m/%d/%Y")
        df = df[df['Location'].notnull() & (df['Location'] != "")]
    return df

def insert_data(df):
# Replace with your details

    # Create SQLAlchemy engine (uses pyodbc under the hood)
    engine = create_engine(
    f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+18+for+SQL+Server"
    )
    # Replace table each time
    print("inserting")
    df.to_sql("IDS_table_v2", con=engine, if_exists="replace", index=False)
    print('Data is inserted to the db')

if __name__ == "__main__":

    insert_logger.info("started")

    final_df = pd.DataFrame()
    # Add the forlder url ID here
    FOLDER_IDS = [
        '1pGKdGtDFPNIsk7fiew4uDEaEuwYi67a_',
        '1QLd_t3udnOyPPf_wy3LsEBQXrNsO_rRE',
        # '1RAJisjI9mbBE-LQWBCyQCUs5FZ7zJaKt' Testing
    ]
    for FOLDER_ID in FOLDER_IDS:
        workbooks = get_sheets_in_folder(FOLDER_ID)
        # print(workbooks)
        for workbook in tqdm(workbooks):
            file_id = workbook['id']
            Practitioner_Name = workbook['name'].split("'")[0]
            Practitioner_Name = Practitioner_Name.replace("*", "").strip()
            file_buffer = download_google_sheet_to_memory(file_id, drive_service)

            xl = pd.ExcelFile(file_buffer)
            sheets = xl.sheet_names
            notfind_logger.warning(f"processing: {workbook['name']}")

            for sheet_name in sheets:
                if sheet_name=='Example' or sheet_name=='Blank Format' or sheet_name=='Compile' or sheet_name=='Validation Lists' or sheet_name=='SNF Validation Lists' or sheet_name=='SNF Validation List' or sheet_name=='Facility Validation':
                    continue
                temp_df = get_sheet_data_Intervention_Reason(xl, sheet_name)
                if temp_df.empty:
                    notfind_logger.warning(f"❌ Target header '{TARGET_HEADER}' not found in workbook: {workbook['name']} any sheet of: {sheet_name}")

    # print(len(temp_df))

                if not temp_df.empty:
                    required_cols = [
    'Id', 'Intervention Reason', 'Billing Date', 'Billed Yes/No', 'Location',
    'SNF/Telehealth', 'Date of Service', 'Note Posted',
    'Patient Name (Last, First)', 'DOB', 'CPT Code',
    'Diagnosis1', 'Diagnosis2', 'Diagnosis3', 'Comments'
    ]

    # Keep only the ones that exist in this sheet
                    available_cols = [col for col in required_cols if col in temp_df.columns]
                    temp_df = temp_df[available_cols].copy()

                    # Add missing columns as empty so schema stays consistent
                    for col in required_cols:
                        if col not in temp_df.columns:
                            temp_df[col] = None

                    # Reorder columns to match required order
                    temp_df = temp_df[required_cols]

                    temp_df['Practitioner_Name'] = Practitioner_Name
                    if Practitioner_Name == '' or Practitioner_Name is None :
                        notfind_logger.warning(f"practitioner name not found in {sheet_name}")
                    final_df = pd.concat([final_df, temp_df], ignore_index=True)

    print(f"Download is completed: {len(final_df)}")

    df = preprocess(final_df)
    df.to_csv('data.csv',index=False)
    insert_data(df)
    insert_logger.info(f"{len(df)} rows inserted")
