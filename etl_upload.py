import os
import time
import requests
import pandas as pd

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload


# --- Variables d'environnement (à définir dans GitHub Actions) ---
API_KEY = os.environ.get("ALPHAVANTAGE_API_KEY")
CLIENT_ID = os.environ.get("CLIENT_ID")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET")
REFRESH_TOKEN = os.environ.get("REFRESH_TOKEN")

# --- Liste des symboles boursiers à extraire ---
symbols = ["IBM", "AAPL", "META", "TSLA"]
all_dataframes = []

# --- Extraction et transformation des données ---
for symbol in symbols:
    print(f"Extraction pour : {symbol}")
    url = (
        f"https://www.alphavantage.co/query"
        f"?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={API_KEY}"
    )
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            time_series = data.get("Time Series (Daily)")
            if time_series:
                # Conversion en DataFrame
                df = pd.DataFrame.from_dict(time_series, orient="index")
                # Renommage colonnes
                df = df.rename(columns={
                    "1. open": "open",
                    "2. high": "high",
                    "3. low": "low",
                    "4. close": "close",
                    "5. volume": "volume"
                })
                df.index.name = "date"
                df.reset_index(inplace=True)
                df["symbol"] = symbol
                all_dataframes.append(df)
                print(f"Données transformées pour {symbol}")
            else:
                print(f"Pas de données disponibles (limite API?) pour {symbol}")
        else:
            print(f"Erreur HTTP {response.status_code} pour {symbol}")
    except Exception as e:
        print(f"Exception lors de l'extraction pour {symbol}: {e}")

    time.sleep(15)  # Pause pour respecter la limite d'appels API

# --- Fusion des données et nettoyage final ---
if all_dataframes:
    final_df = pd.concat(all_dataframes, ignore_index=True)

    # Conversion des types
    final_df["date"] = pd.to_datetime(final_df["date"])
    final_df[["open", "high", "low", "close"]] = final_df[["open", "high", "low", "close"]].astype(float)
    final_df["volume"] = final_df["volume"].astype(int)

    # Tri et réorganisation des colonnes
    final_df = final_df.sort_values(by=["symbol", "date"], ascending=[True, True])
    final_df = final_df[["date", "open", "high", "low", "close", "volume", "symbol"]]

    # --- Enregistrement au format Excel ---
    excel_filename = "bourses.xlsx"
    with pd.ExcelWriter(excel_filename, engine="xlsxwriter") as writer:
        final_df.to_excel(writer, index=False, sheet_name="Bourseupdate")

        # Mise en forme du fichier Excel
        workbook = writer.book
        worksheet = writer.sheets["Bourseupdate"]

        # Format date personnalisé
        date_format = workbook.add_format({'num_format': 'yyyy-mm-dd'})
        worksheet.set_column(0, 0, 15, date_format)

        # Auto-ajustement largeur colonnes
        for i, column in enumerate(final_df.columns):
            max_width = max(final_df[column].astype(str).map(len).max(), len(column)) + 2
            worksheet.set_column(i, i, max_width)

    print(f"Données sauvegardées localement dans {excel_filename}")

    # --- Authentification Google Drive ---
    creds = Credentials(
        None,
        refresh_token=REFRESH_TOKEN,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        token_uri="https://oauth2.googleapis.com/token"
    )

    if creds.expired and creds.refresh_token:
        creds.refresh(Request())

    service = build("drive", "v3", credentials=creds)

    # --- Recherche fichier existant sur Google Drive ---
    query = (
        f"name='{excel_filename}' and "
        f"mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' and trashed=false"
    )
    results = service.files().list(q=query, fields="files(id, name)").execute()
    items = results.get('files', [])

    media = MediaFileUpload(excel_filename, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    # --- Mise à jour ou création du fichier sur Google Drive ---
    if items:
        file_id = items[0]['id']
        service.files().update(fileId=file_id, media_body=media).execute()
        print(f"Fichier mis à jour sur Google Drive (ID: {file_id})")
    else:
        file_metadata = {
            "name": excel_filename,
            "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        }
        uploaded_file = service.files().create(body=file_metadata, media_body=media).execute()
        print(f"Nouveau fichier créé sur Google Drive (ID: {uploaded_file.get('id')})")

else:
    print("Aucune donnée extraite.")
