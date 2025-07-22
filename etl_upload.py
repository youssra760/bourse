import os
import pandas as pd
import requests
import time

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import gspread
import gspread_dataframe as gd
from oauth2client.service_account import ServiceAccountCredentials

# 1️⃣ — ETAPE 1 : Extraction des données (exemple API)
try:
    # Simuler appel API (à remplacer par ton appel réel)
    response = requests.get("https://api.exemple.com/data")
    response.raise_for_status()
    data = response.json()

    # Exemple simple de transformation
    df = pd.DataFrame(data)

    # Sauvegarder dans un fichier local CSV
    csv_file = "data_bourse.csv"
    df.to_csv(csv_file, index=False)
    print("✅ Données extraites et sauvegardées localement.")

except Exception as e:
    print(f"❌ Erreur d’extraction : {e}")
    exit(1)

# 2️⃣ — ETAPE 2 : Upload dans Google Drive
try:
    creds_drive = Credentials.from_service_account_file(
        "credentials.json",
        scopes=["https://www.googleapis.com/auth/drive"]
    )
    drive_service = build("drive", "v3", credentials=creds_drive)

    folder_id = os.environ.get("GDRIVE_FOLDER_ID")  # à définir dans les secrets GitHub

    file_metadata = {
        "name": csv_file,
        "parents": [folder_id],
        "mimeType": "application/vnd.google-apps.spreadsheet"
    }

    media = MediaFileUpload(csv_file, mimetype="text/csv", resumable=True)
    uploaded = drive_service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id"
    ).execute()

    print(f"✅ Fichier CSV uploadé sur Google Drive avec ID : {uploaded.get('id')}")

except Exception as e:
    print(f"❌ Erreur lors de l’upload Google Drive : {e}")
    exit(1)

# 3️⃣ — ETAPE 3 : Mise à jour de Google Sheets
try:
    # Connexion à Google Sheets
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']

    credentials_sheets = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    client = gspread.authorize(credentials_sheets)

    spreadsheet = client.open("Nom_de_ta_Google_Sheet")  # change le nom ici
    worksheet = spreadsheet.worksheet("Feuille1")

    worksheet.clear()
    gd.set_with_dataframe(worksheet, df)

    print("✅ Google Sheet mise à jour avec succès.")

except Exception as e:
    print(f"❌ Erreur lors de la mise à jour Google Sheets : {e}")
    exit(1)
