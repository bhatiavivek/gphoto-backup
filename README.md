# Google Photos Backup

This script provides a simple way to back up your Google Photos library to your local machine. It organizes photos by date and maintains album structures using symlinks.

## Features

- Downloads photos from Google Photos to your local machine
- Organizes photos into folders by year and month (YYYY-MM format)
- Maintains album structure using symlinks
- Filters out WhatsApp images (matching pattern IMG-*-WA*.jpg)
- Resumes interrupted downloads
- Logs operations for easy tracking and debugging

## Prerequisites

Before you can use this script, you need to:

1. Have a Google account with photos you want to back up
2. Register for the Google Photos API
3. Create a project in the Google Developers Console
4. Enable the Photos Library API for your project
5. Create credentials (OAuth client ID) for a desktop application
6. Download the client configuration and save it as `client_secret.json` in the same directory as the script
7. **For the time being, this is Linux-only. It should work on the Mac although it hasn't been tested. Windows support is ccoming, likely before the end of June 2024**

## Installation

1. Clone this repository:
   ```
   git clone https://github.com/yourusername/google-photos-backup.git
   ```

2. Navigate to the project directory:
   ```
   cd google-photos-backup
   ```

3. Install the required Python packages:
   ```
   pip install -r requirements.txt
   ```

4. Place your `client_secret.json` file in the project directory.

## Usage

Run the script with:

```
python google_photos_backup.py
```

On first run, you'll be prompted to authorize the application. Follow the URL provided to grant permission, then copy and paste the authorization code back into the terminal.

## Configuration

You can modify the following variables in the script:

- `download_dir`: The directory where photos will be downloaded and organized
- `db_file`: The SQLite database file used to track downloads and organization
- `start_date` and `end_date`: The date range for photos to download (default is last 30 days)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the [MIT License](LICENSE).

## Disclaimer

This script is not officially associated with Google. Use it at your own risk. Always ensure you have multiple backups of important data.