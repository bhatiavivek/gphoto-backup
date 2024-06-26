"""
Google Photos Sync and Organize Script

This script synchronizes photos from Google Photos to a local directory and organizes them.
It uses the Google Photos API to fetch photos and albums, downloads them, and organizes
them into date-based folders and album-based symlinks.

Requirements:
- Google Cloud project with Photos API enabled
- OAuth 2.0 credentials (client_secret.json)
- Required Python packages (see imports)

Usage:
python script_name.py
"""

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
import shutil

LOGGING_ENABLED = True
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
    """
    Synchronizes photos from Google Photos to a local directory within a specified date range.

    This function performs the following operations:
    1. Fetches and stores all albums and their media items.
    2. Retrieves media items within the specified date range.
    3. Downloads each media item and stores it locally.
    4. Associates downloaded items with their respective albums in the database.

    The function handles pagination and can be interrupted safely.

    Args:
    download_dir (str): Path to the directory where photos will be downloaded.
    start_date (datetime): The start date of the range to sync.
    end_date (datetime): The end date of the range to sync.
    db_file (str): Path to the SQLite database file for storing metadata.

    Global Variables Used:
    interrupted (bool): Flag to signal if the sync process should be interrupted.

    Note:
    This function may take a considerable amount of time to execute depending on the
    number of photos in the specified date range and the network speed.
    """
    # Initialize credentials and database connection
    creds = get_credentials()
    session = google.auth.transport.requests.AuthorizedSession(creds)
    conn = init_database(db_file)
    cursor = conn.cursor()

    try:
        # Fetch and store albums and their media items
        albums, media_item_to_albums = fetch_albums_with_media_items(session)
        for album in albums:
            add_album(cursor, album)
        conn.commit()
        logger.info(f"Fetched and stored {len(albums)} albums")

        # Set up the API request for fetching media items
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

        # Main loop for fetching and downloading media items
        while not interrupted:
            try:
                # Make API request to get media items
                response = make_api_request(session, url, method="post", json=body)
                data = response.json()
                items = data.get("mediaItems", [])

                # Process each media item
                for item in items:
                    if interrupted:
                        break
                    try:
                        # Download the photo and store it locally
                        download_photo(item, download_dir, cursor)
                        # Associate the item with its albums in the database
                        for album_id in media_item_to_albums.get(item["id"], []):
                            add_item_to_album(cursor, album_id, item["id"])
                    except Exception as e:
                        logger.error(
                            f"Error processing item {item.get('id', 'unknown')}: {str(e)}"
                        )
                        continue

                conn.commit()

                # Check for more pages of results
                if "nextPageToken" in data:
                    body["pageToken"] = data["nextPageToken"]
                else:
                    break  # No more pages, exit the loop

            except requests.exceptions.RequestException as e:
                logger.error(f"Error in API request: {str(e)}")
                if "pageToken" in body:
                    del body["pageToken"]  # Reset page token on error

    finally:
        # Ensure database connection is closed
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
    """
    Fetches all albums and their associated media items from Google Photos.

    This function performs two main operations:
    1. Retrieves all albums from the user's Google Photos account.
    2. For each album, fetches all media items contained within it.

    The function handles pagination for both album and media item retrieval,
    ensuring all data is collected even if it spans multiple pages.

    Args:
    session (google.auth.transport.requests.AuthorizedSession): An authorized session for making API requests.

    Returns:
    tuple: A tuple containing two elements:
        - list: All fetched albums, each as a dictionary of album metadata.
        - dict: A mapping of media item IDs to lists of album IDs they belong to.

    Note:
    This function may take a considerable amount of time to execute for accounts
    with many albums or media items. It logs its progress for monitoring.
    """
    url = "https://photoslibrary.googleapis.com/v1/albums"
    albums = []
    media_item_to_albums = {}
    page_token = None
    album_count = 0
    total_media_items = 0

    logger.info("Starting to fetch albums and their media items")

    while True:
        # Set up parameters for album retrieval, including pagination
        params = {"pageSize": 50}
        if page_token:
            params["pageToken"] = page_token

        logger.debug(f"Fetching albums page with params: {params}")
        response = make_api_request(session, url, params=params)
        data = response.json()

        # Process the albums from the current page
        page_albums = data.get("albums", [])
        album_count += len(page_albums)
        logger.info(f"Fetched {len(page_albums)} albums (Total: {album_count})")

        for album in page_albums:
            albums.append(album)
            logger.debug(f"Processing album: {album['title']} (ID: {album['id']})")

            # Fetch media items for the current album
            media_items_url = (
                "https://photoslibrary.googleapis.com/v1/mediaItems:search"
            )
            media_items_body = {"albumId": album["id"], "pageSize": 100}
            media_items_page_token = None
            album_media_items_count = 0

            # Paginate through all media items in the album
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

                # Process media items from the current page
                page_media_items = media_items_data.get("mediaItems", [])
                album_media_items_count += len(page_media_items)
                total_media_items += len(page_media_items)

                # Map each media item to its album
                for item in page_media_items:
                    if item["id"] not in media_item_to_albums:
                        media_item_to_albums[item["id"]] = []
                    media_item_to_albums[item["id"]].append(album["id"])

                # Check for more pages of media items
                media_items_page_token = media_items_data.get("nextPageToken")
                if not media_items_page_token:
                    break

            logger.info(
                f"Fetched {album_media_items_count} media items for album: {album['title']}"
            )

        # Check for more pages of albums
        page_token = data.get("nextPageToken")
        if not page_token:
            break

    # Log summary of fetched data
    logger.info(
        f"Finished fetching albums and media items. Total albums: {album_count}, Total media items: {total_media_items}"
    )
    logger.info(
        f"Number of unique media items across all albums: {len(media_item_to_albums)}"
    )

    return albums, media_item_to_albums


def organize_photos(download_dir, db_file):
    """
    Organizes downloaded photos into a structured folder hierarchy and creates album symlinks.

    This function:
    1. Moves photos from the download directory into year-month folders.
    2. Creates symlinks in album folders pointing to the organized photos.

    Folder structure created:

    download_dir/
    ├── YYYY-MM/
    │   ├── photo1.jpg
    │   ├── photo2.jpg
    │   └── ...
    ├── YYYY-MM/
    │   ├── photo3.jpg
    │   ├── photo4.jpg
    │   └── ...
    └── Albums/
        ├── Album1/
        │   ├── photo1.jpg -> ../../YYYY-MM/photo1.jpg
        │   └── photo2.jpg -> ../../YYYY-MM/photo2.jpg
        └── Album2/
            ├── photo3.jpg -> ../../YYYY-MM/photo3.jpg
            └── photo4.jpg -> ../../YYYY-MM/photo4.jpg

    Args:
    download_dir (str): Path to the directory where photos are initially downloaded.
    db_file (str): Path to the SQLite database file containing photo metadata.

    The function doesn't return anything but logs its actions using the logger.

    Note:
    - Photos not associated with any album will only exist in the YYYY-MM folders.
    - The function skips WhatsApp images (matching pattern IMG-*-WA*.jpg).
    - If a photo has already been organized, it won't be moved again.
    """

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Fetch all photos with their creation time and albums from the local db
    cursor.execute(
        """
        SELECT df.filename, df.creation_time, GROUP_CONCAT(a.title, '|') as albums
        FROM downloaded_files df
        LEFT JOIN album_items ai ON df.file_id = ai.file_id
        LEFT JOIN albums a ON ai.album_id = a.album_id
        GROUP BY df.file_id
    """
    )

    for filename, creation_time, albums in cursor.fetchall():
        # Parse creation time
        if creation_time:
            date = datetime.fromisoformat(creation_time.split("+")[0])
            date_folder = f"{date.year:04d}-{date.month:02d}"
        else:
            date_folder = "Unknown_Date"

        # Ensure date folder exists
        date_path = os.path.join(download_dir, date_folder)
        os.makedirs(date_path, exist_ok=True)

        # Check if file is in original location or already moved
        old_path = os.path.join(download_dir, filename)
        new_path = os.path.join(date_path, filename)

        if os.path.exists(old_path):
            # File is in original location, move it
            shutil.move(old_path, new_path)
            logger.info(f"Moved {filename} to {date_folder}")
        elif not os.path.exists(new_path):
            # File is not in original location or new location, log it
            logger.warning(f"File not found: {filename}")
            continue

        # Handle albums
        if albums:
            for album in albums.split("|"):
                album_path = os.path.join(download_dir, "Albums", album)
                os.makedirs(album_path, exist_ok=True)
                # Create symlink in album folder if not already there
                album_file_path = os.path.join(album_path, filename)
                if not os.path.exists(album_file_path):
                    os.symlink(new_path, album_file_path)
                    logger.info(f"Created symlink for {filename} in album {album}")

    conn.close()

    logger.info("Photos organization completed")


if __name__ == "__main__":
    download_dir = "/home/vivek/gphotos-backup"
    db_file = "photo_sync.db"
    start_date = datetime.now() - timedelta(days=365)
    end_date = datetime.now()  # Today
    start_date = datetime(2010, 1, 1)
    end_date = datetime(2016, 1, 2)

    try:
        sync_photos(download_dir, start_date, end_date, db_file)
        organize_photos(download_dir, db_file)
    except Exception as e:
        logger.exception(f"An unexpected error occurred: {str(e)}")
