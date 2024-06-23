from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import google.auth.transport.requests
import requests
import os
from datetime import datetime, timedelta
import json

# Set up credentials
SCOPES = ["https://www.googleapis.com/auth/photoslibrary.readonly"]
API_SERVICE_NAME = "photoslibrary"
API_VERSION = "v1"


def get_credentials():
    creds = None
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                "client_secret.json", SCOPES
            )
            creds = flow.run_local_server(port=0)
        with open("token.json", "w") as token:
            token.write(creds.to_json())
    return creds


# Function to download photos
def download_photo(item, download_dir):
    filename = f"{item['filename']}"
    file_path = os.path.join(download_dir, filename)

    if not os.path.exists(file_path):
        url = item["baseUrl"] + "=d"
        response = requests.get(url)
        if response.status_code == 200:
            with open(file_path, "wb") as f:
                f.write(response.content)
            print(f"Downloaded: {filename}")
        else:
            print(f"Failed to download: {filename}")


# Main sync function with date range
def sync_photos(download_dir, start_date, end_date):
    creds = get_credentials()
    session = google.auth.transport.requests.AuthorizedSession(creds)

    url = f"https://photoslibrary.googleapis.com/{API_VERSION}/mediaItems:search"

    body = {
        "pageSize": 100,
        "filters": {
            "dateFilter": {
                "ranges": [
                    {
                        "startDate": {
                            "year": start_date.year,
                            "month": start_date.month,
                            "day": start_date.day,
                        },
                        "endDate": {
                            "year": end_date.year,
                            "month": end_date.month,
                            "day": end_date.day,
                        },
                    }
                ]
            }
        },
    }

    while True:
        response = session.post(url, data=json.dumps(body))
        if response.status_code != 200:
            print(f"Error: {response.status_code} - {response.text}")
            break

        data = response.json()
        items = data.get("mediaItems", [])

        for item in items:
            download_photo(item, download_dir)

        if "nextPageToken" in data:
            body["pageToken"] = data["nextPageToken"]
        else:
            break


# Run the sync with date range
download_dir = "/home/vivek/gphotos-backup"
start_date = datetime.now() - timedelta(days=7)  # 7 days ago
end_date = datetime.now()  # Today

sync_photos(download_dir, start_date, end_date)
