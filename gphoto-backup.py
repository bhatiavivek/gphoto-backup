from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import google.auth.transport.requests
import requests
import os
from datetime import datetime, timedelta, timezone
import json
import sqlite3
import logging
from logging.handlers import RotatingFileHandler
import sys
import signal
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
    after_log,
)
import requests.exceptions

LOGGING_ENABLED = True  # Set this to False to disable logging
LOG_FILE = "photo_sync.log"
CONSOLE_LOG_LEVEL = logging.INFO
FILE_LOG_LEVEL = logging.DEBUG


def setup_logging(log_file, console_level, file_level):
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(console_level)
    console_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # File handler
    file_handler = RotatingFileHandler(
        log_file, maxBytes=10 * 1024 * 1024, backupCount=5
    )
    file_handler.setLevel(file_level)
    file_formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s"
    )
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    return logger


if LOGGING_ENABLED:
    logger = setup_logging(LOG_FILE, CONSOLE_LOG_LEVEL, FILE_LOG_LEVEL)
else:
    logger = logging.getLogger()
    logger.addHandler(logging.NullHandler())

# Global flag for interruption
interrupted = False


def signal_handler(signum, frame):
    global interrupted
    interrupted = True
    logger.warning(
        "Interruption signal received. Finishing current operation and exiting..."
    )


signal.signal(signal.SIGINT, signal_handler)


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

    # Create albums table
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS albums (
            album_id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            item_count INTEGER,
            cover_photo_id TEXT
        )
    """
    )

    # Create album_items table for many-to-many relationship
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS album_items (
            album_id TEXT,
            file_id TEXT,
            PRIMARY KEY (album_id, file_id),
            FOREIGN KEY (album_id) REFERENCES albums (album_id),
            FOREIGN KEY (file_id) REFERENCES downloaded_files (file_id)
        )
    """
    )

    conn.commit()
    return conn


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type(
        (requests.exceptions.RequestException, requests.exceptions.HTTPError)
    ),
    before_sleep=before_sleep_log(logger, logging.INFO),
    after=after_log(logger, logging.INFO),
)
def make_api_request(session, url, method="get", **kwargs):
    try:
        response = session.request(method, url, **kwargs)
        response.raise_for_status()
        return response
    except (requests.exceptions.RequestException, requests.exceptions.HTTPError) as e:
        logger.info(f"API request failed: {str(e)}")
        raise


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    before_sleep=before_sleep_log(logger, logging.INFO),
    after=after_log(logger, logging.INFO),
)
def download_file(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.content
    except requests.exceptions.RequestException as e:
        logger.info(f"File download failed: {str(e)}")
        raise


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type(
        (requests.exceptions.RequestException, requests.exceptions.HTTPError)
    ),
    before_sleep=before_sleep_log(logger, logging.INFO),
    after=after_log(logger, logging.INFO),
)
def fetch_albums(session):
    url = "https://photoslibrary.googleapis.com/v1/albums"
    albums = []
    page_token = None

    while True:
        params = {"pageSize": 50}
        if page_token:
            params["pageToken"] = page_token

        response = make_api_request(session, url, params=params)
        data = response.json()

        albums.extend(data.get("albums", []))
        page_token = data.get("nextPageToken")

        if not page_token:
            break

    return albums


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

    cursor.execute("SELECT 1 FROM downloaded_files WHERE file_id = ?", (file_id,))
    if cursor.fetchone():
        logger.info(f"Skipping already downloaded file: {filename}")
        return

    url = item["baseUrl"] + "=d"
    logger.debug(f"Attempting to download {filename} from URL: {url}")
    try:
        content = download_file(url)
        with open(file_path, "wb") as f:
            f.write(content)
        logger.info(f"Downloaded: {filename}")
        add_downloaded_file(cursor, item, filename)
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download {filename} after retries: {str(e)}")


def sync_photos(download_dir, start_date, end_date, db_file):
    creds = get_credentials()
    session = google.auth.transport.requests.AuthorizedSession(creds)

    conn = init_database(db_file)
    cursor = conn.cursor()

    try:
        # Fetch albums and create media item to albums mapping
        albums, media_item_to_albums = fetch_albums_with_media_items(session)
        for album in albums:
            add_album(cursor, album)
        conn.commit()
        logger.info(f"Fetched and stored {len(albums)} albums")

        # Existing photo sync code...
        url = "https://photoslibrary.googleapis.com/v1/mediaItems:search"
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

        while not interrupted:
            try:
                response = make_api_request(session, url, method="post", json=body)
                data = response.json()
                items = data.get("mediaItems", [])

                for item in items:
                    if interrupted:
                        break
                    download_photo(item, download_dir, cursor)

                    # Add album associations
                    for album_id in media_item_to_albums.get(item["id"], []):
                        add_item_to_album(cursor, album_id, item["id"])

                conn.commit()

                if "nextPageToken" in data:
                    body["pageToken"] = data["nextPageToken"]
                else:
                    break
            except requests.exceptions.RequestException as e:
                logger.error(f"Error in API request: {str(e)}")
                if "pageToken" in body:
                    del body["pageToken"]  # Reset page token on error
    finally:
        conn.close()
        if interrupted:
            logger.info("Script interrupted. Exiting gracefully.")
        else:
            logger.info("Sync completed successfully.")


def add_album(cursor, album):
    cursor.execute(
        """
        INSERT OR REPLACE INTO albums (album_id, title, item_count, cover_photo_id)
        VALUES (?, ?, ?, ?)
    """,
        (
            album["id"],
            album["title"],
            album.get("mediaItemsCount"),
            album.get("coverPhotoMediaItemId"),
        ),
    )


def add_item_to_album(cursor, album_id, file_id):
    cursor.execute(
        """
        INSERT OR IGNORE INTO album_items (album_id, file_id)
        VALUES (?, ?)
    """,
        (album_id, file_id),
    )


def fetch_albums_with_media_items(session):
    url = "https://photoslibrary.googleapis.com/v1/albums"
    albums = []
    media_item_to_albums = {}
    page_token = None
    album_count = 0
    total_media_items = 0

    logger.info("Starting to fetch albums and their media items")

    while True:
        params = {"pageSize": 50}
        if page_token:
            params["pageToken"] = page_token

        logger.debug(f"Fetching albums page with params: {params}")
        response = make_api_request(session, url, params=params)
        data = response.json()

        page_albums = data.get("albums", [])
        album_count += len(page_albums)
        logger.info(f"Fetched {len(page_albums)} albums (Total: {album_count})")

        for album in page_albums:
            albums.append(album)
            logger.debug(f"Processing album: {album['title']} (ID: {album['id']})")

            # Fetch media items for this album
            media_items_url = (
                "https://photoslibrary.googleapis.com/v1/mediaItems:search"
            )
            media_items_body = {"albumId": album["id"], "pageSize": 100}
            media_items_page_token = None
            album_media_items_count = 0

            while True:
                if media_items_page_token:
                    media_items_body["pageToken"] = media_items_page_token

                logger.debug(
                    f"Fetching media items for album {album['title']} with body: {media_items_body}"
                )
                media_items_response = make_api_request(
                    session, media_items_url, method="post", json=media_items_body
                )
                media_items_data = media_items_response.json()

                page_media_items = media_items_data.get("mediaItems", [])
                album_media_items_count += len(page_media_items)
                total_media_items += len(page_media_items)

                for item in page_media_items:
                    if item["id"] not in media_item_to_albums:
                        media_item_to_albums[item["id"]] = []
                    media_item_to_albums[item["id"]].append(album["id"])

                media_items_page_token = media_items_data.get("nextPageToken")
                if not media_items_page_token:
                    break

            logger.info(
                f"Fetched {album_media_items_count} media items for album: {album['title']}"
            )

        page_token = data.get("nextPageToken")
        if not page_token:
            break

    logger.info(
        f"Finished fetching albums and media items. Total albums: {album_count}, Total media items: {total_media_items}"
    )
    logger.info(
        f"Number of unique media items across all albums: {len(media_item_to_albums)}"
    )

    return albums, media_item_to_albums


if __name__ == "__main__":
    download_dir = "/home/vivek/gphotos-backup"
    db_file = "photo_sync.db"
    start_date = datetime.now() - timedelta(days=5)
    end_date = datetime.now()  # Today

    try:
        sync_photos(download_dir, start_date, end_date, db_file)
    except Exception as e:
        logger.exception(f"An unexpected error occurred: {str(e)}")
