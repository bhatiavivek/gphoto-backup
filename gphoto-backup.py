from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import google.auth.transport.requests
import requests
import os
from datetime import datetime, timedelta, timezone
import json
import sqlite3


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


def init_database(db_file):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS downloaded_files (
            file_id TEXT PRIMARY KEY,
            filename TEXT NOT NULL,
            download_date TEXT NOT NULL,
            creation_time TEXT,
            width INTEGER,
            height INTEGER,
            photo_type TEXT,
            camera_make TEXT,
            camera_model TEXT,
            focal_length FLOAT,
            aperture_fnumber FLOAT,
            iso_equivalent INTEGER,
            exposure_time TEXT,
            lat FLOAT,
            long FLOAT
        )
    """
    )
    conn.commit()
    return conn


# Function to check if a file has been downloaded
def is_file_downloaded(cursor, file_id):
    cursor.execute("SELECT 1 FROM downloaded_files WHERE file_id = ?", (file_id,))
    return cursor.fetchone() is not None


# Function to add a downloaded file to the database
def add_downloaded_file(cursor, item, filename):
    file_id = item["id"]
    metadata = item.get("mediaMetadata", {})

    creation_time = metadata.get("creationTime")
    if creation_time:
        # Parse the ISO format string to a datetime object
        creation_time = datetime.fromisoformat(creation_time.replace("Z", "+00:00"))
        # Ensure it's in UTC
        creation_time = creation_time.astimezone(timezone.utc)

    photo = metadata.get("photo", {})

    cursor.execute(
        """
        INSERT OR REPLACE INTO downloaded_files 
        (file_id, filename, download_date, creation_time, width, height, 
        photo_type, camera_make, camera_model, focal_length, aperture_fnumber, 
        iso_equivalent, exposure_time, lat, long)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        (
            file_id,
            filename,
            datetime.now(timezone.utc).isoformat(),
            creation_time.isoformat() if creation_time else None,
            metadata.get("width"),
            metadata.get("height"),
            metadata.get("mimeType"),
            photo.get("cameraMake"),
            photo.get("cameraModel"),
            photo.get("focalLength"),
            photo.get("apertureFNumber"),
            photo.get("isoEquivalent"),
            photo.get("exposureTime"),
            item.get("geoData", {}).get("latitude"),
            item.get("geoData", {}).get("longitude"),
        ),
    )


def download_photo(item, download_dir, cursor):
    filename = f"{item['filename']}"
    file_path = os.path.join(download_dir, filename)
    file_id = item["id"]

    # Check if the file has already been downloaded
    cursor.execute("SELECT 1 FROM downloaded_files WHERE file_id = ?", (file_id,))
    if cursor.fetchone():
        print(f"Skipping already downloaded file: {filename}")
        return

    url = item["baseUrl"] + "=d"
    response = requests.get(url)
    if response.status_code == 200:
        with open(file_path, "wb") as f:
            f.write(response.content)
        print(f"Downloaded: {filename}")

        # Update tracking data in SQLite with extended metadata
        add_downloaded_file(cursor, item, filename)
    else:
        print(f"Failed to download: {filename}")


# Modified main sync function
def sync_photos(download_dir, start_date, end_date, db_file):
    creds = get_credentials()
    session = google.auth.transport.requests.AuthorizedSession(creds)

    url = f"https://photoslibrary.googleapis.com/v1/mediaItems:search"

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

    conn = init_database(db_file)
    cursor = conn.cursor()

    try:
        while True:
            response = session.post(url, data=json.dumps(body))
            if response.status_code != 200:
                print(f"Error: {response.status_code} - {response.text}")
                break

            data = response.json()
            items = data.get("mediaItems", [])

            for item in items:
                download_photo(item, download_dir, cursor)

            # Commit changes after each batch
            conn.commit()

            if "nextPageToken" in data:
                body["pageToken"] = data["nextPageToken"]
            else:
                break
    finally:
        conn.close()


# Run the sync with date range
download_dir = "/home/vivek/gphotos-backup"
db_file = "photo_sync.db"
start_date = datetime.now() - timedelta(days=7)
end_date = datetime.now()  # Today

sync_photos(download_dir, start_date, end_date, db_file)
