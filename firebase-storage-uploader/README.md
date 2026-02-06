# Firebase Storage Uploader

Upload files to Firebase Storage and get public download URLs.

## Installation

```bash
pip install -r requirements.txt
```

## Usage

```bash
python firebase_upload.py --env /path/to/.env --source /path/to/file.zip --dest folder/file.zip
```

## Arguments

| Argument | Short | Required | Description |
|----------|-------|----------|-------------|
| `--env` | `-e` | Yes | Path to .env file with Firebase credentials |
| `--source` | `-s` | Yes | Local file path to upload |
| `--dest` | `-d` | Yes | Destination path in Firebase Storage |
| `--no-public` | | No | Generate signed URL instead of public URL |

## Required .env Variables

```env
FB_PROJECT_ID=your-project-id
FB_PRIVATE_KEY=-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n
FB_CLIENT_EMAIL=firebase-adminsdk-xxx@your-project.iam.gserviceaccount.com
FB_STORAGE_BUCKET=your-project.appspot.com
```

## Optional .env Variables

```env
FB_PROJECT_KEY_ID=...
FB_CLIENT_ID=...
FB_AUTH_URI=https://accounts.google.com/o/oauth2/auth
FB_TOKEN_URI=https://oauth2.googleapis.com/token
FB_AUTH_PROVIDER_X=https://www.googleapis.com/oauth2/v1/certs
FB_CLIENT_X=https://www.googleapis.com/robot/v1/metadata/x509/...
FB_UNIVERSE_DOMAIN=googleapis.com
```

## Examples

```bash
# Upload and get public URL
python firebase_upload.py -e .env -s backup.zip -d backups/2024-01-01.zip

# Upload and get signed URL (7 days)
python firebase_upload.py -e .env -s private.zip -d private/data.zip --no-public
```
