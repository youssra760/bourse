import os
import requests
import time
import pandas as pd

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

# ✅ Variables d'environnement depuis GitHub Actions secrets
API_KEY = os.environ.get("ALPHAVANTAGE_API_KEY")
CLIENT_ID = os.environ.get("CLIENT_ID")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET")
REFRESH_TOKEN = os.environ.get("REFRESH_TOKEN")

# ✅ ID du Google Sheets où les données seront écrites
SPREADSHEET_ID = "1S0WTG-AVXhaVLhSKxOJAngLDvu5rEwmgZOTIfXqwGzU"
RANGE_NAME = "Feuille1!A1"  # À adapter si l'onglet a été renommé

# 🏢 Symboles boursiers à extraire
symbols = ["IBM", "AAPL", "META", "TSLA"]
all_dataframes = []

# 🔄 Extraction des données pour chaque symbole
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
                print(f"⚠️ Pas de données (limite API ?) pour {symbol}")
        else:
            print(f"❌ Erreur HTTP {response.status_code} pour {symbol}")
    except Exception as e:
        print(f"❌ Exception pour {symbol}: {e}")
    time.sleep(15)  # respecter la limite d'appel API d'Alpha Vantage

# 📊 Fusion, nettoyage et tri final
if all_dataframes:
    final_df = pd.concat(all_dataframes, ignore_index=True)
    final_df["date"] = pd.to_datetime(final_df["date"])
    final_df[["open", "high", "low", "close"]] = final_df[["open", "high", "low", "close"]].astype(float)
    final_df["volume"] = final_df["volume"].astype(int)
    final_df = final_df.sort_values(by=["symbol", "date"], ascending=[True, True])
    final_df = final_df[["date", "open", "high", "low", "close", "volume", "symbol"]]

    print("📋 Données prêtes à être envoyées vers Google Sheets")

    # 🔐 Création des credentials OAuth 2.0
    creds = Credentials(
        None,
        refresh_token=REFRESH_TOKEN,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        token_uri="https://oauth2.googleapis.com/token"
    )

    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())

    # 📤 Envoi vers Google Sheets
    sheets_service = build("sheets", "v4", credentials=creds)

    # Préparer les données (entêtes + lignes)
    values = [final_df.columns.tolist()] + final_df.values.tolist()

    # Vider l’ancienne feuille
    sheets_service.spreadsheets().values().clear(
        spreadsheetId=SPREADSHEET_ID,
        range=RANGE_NAME
    ).execute()

    # Écrire les nouvelles données
    sheets_service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=RANGE_NAME,
        valueInputOption="RAW",
        body={"values": values}
    ).execute()

    print("✅ Données mises à jour dans Google Sheets avec succès !")
else:
    print("⚠️ Aucune donnée à sauvegarder")

        ).execute()
        print(f"✅ Upload réussi ! File ID: {uploaded_file.get('id')}")
else:
    print("❌ Aucune donnée à sauvegarder")
