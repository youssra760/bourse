import os
import requests
import time
import pandas as pd

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# ✅ Variables d'environnement depuis GitHub Actions secrets
API_KEY = os.environ.get("ALPHAVANTAGE_API_KEY")
CLIENT_ID = os.environ.get("CLIENT_ID")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET")
REFRESH_TOKEN = os.environ.get("REFRESH_TOKEN")

# 🏢 Symboles à extraire
symbols = ["IBM", "AAPL", "META", "TSLA"]

all_dataframes = []

for symbol in symbols:
    print(f"🔄 Extraction pour : {symbol}")
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={API_KEY}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            time_series = data.get("Time Series (Daily)")
            if time_series:
                df = pd.DataFrame.from_dict(time_series, orient="index")
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
                print(f"✅ Données transformées pour {symbol}")
            else:
                print(f"⚠ Pas de données (limite API?) pour {symbol}")
        else:
            print(f"❌ Erreur HTTP {response.status_code} pour {symbol}")
    except Exception as e:
        print(f"❌ Exception pour {symbol}: {e}")
    time.sleep(15)  # respecter la limite API

# ✅ Sauvegarde dans un fichier CSV
if all_dataframes:
    final_df = pd.concat(all_dataframes, ignore_index=True)
    final_df["date"] = pd.to_datetime(final_df["date"])
    final_df[["open", "high", "low", "close"]] = final_df[["open", "high", "low", "close"]].astype(float)
    final_df["volume"] = final_df["volume"].astype(int)

    # 🔀 Trier par symbole puis date
    final_df = final_df.sort_values(by=["symbol", "date"], ascending=[True, True])

    # 📋 Réorganiser les colonnes dans le bon ordre
    final_df = final_df[["date", "open", "high", "low", "close", "volume", "symbol"]]

    csv_filename = "bourses.csv"
    final_df.to_csv(csv_filename, index=False)
    print("💾 Données sauvegardées localement dans bourses.csv")

    # ✅ Upload vers Google Drive
    print("☁ Upload vers Google Drive...")

    creds = Credentials(
        None,
        refresh_token=REFRESH_TOKEN,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        token_uri="https://oauth2.googleapis.com/token"
    )

    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())

    service = build("drive", "v3", credentials=creds)

    # Chercher s'il existe déjà un fichier avec le même nom
    query = f"name='{csv_filename}' and mimeType='text/csv' and trashed=false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    items = results.get('files', [])

    media = MediaFileUpload(csv_filename, mimetype="text/csv")

    if items:
        # Fichier existe → update
        file_id = items[0]['id']
        updated_file = service.files().update(
            fileId=file_id,
            media_body=media
        ).execute()
        print(f"♻ Fichier mis à jour sur Google Drive (ID: {file_id})")
    else:
        # Fichier n'existe pas → create
        file_metadata = {"name": csv_filename, "mimeType": "text/csv"}
        uploaded_file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields="id"
        ).execute()
        print(f"✅ Upload réussi ! File ID: {uploaded_file.get('id')}")
else:
    print("❌ Aucune donnée à sauvegarder")
