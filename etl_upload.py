from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

CLIENT_ID = os.environ.get("CLIENT_ID")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET")
REFRESH_TOKEN = os.environ.get("REFRESH_TOKEN")
SPREADSHEET_ID = "1S0WTG-AVXhaVLhSKxOJAngLDvu5rEwmgZOTIfXqwGzU"
RANGE_NAME = "Feuille1!A1"

creds = Credentials(
    None,
    refresh_token=REFRESH_TOKEN,
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    token_uri="https://oauth2.googleapis.com/token"
)

if creds.expired and creds.refresh_token:
    creds.refresh(Request())

service = build('sheets', 'v4', credentials=creds)
values = [
    ["Test", "Data"],
    [1, 2],
    [3, 4],
]

# Clear large range
service.spreadsheets().values().clear(
    spreadsheetId=SPREADSHEET_ID,
    range="Feuille1!A1:Z100"
).execute()

# Update values
service.spreadsheets().values().update(
    spreadsheetId=SPREADSHEET_ID,
    range=RANGE_NAME,
    valueInputOption="RAW",
    body={"values": values}
).execute()

print("Test d’écriture terminé avec succès.")
