#!/usr/bin/env python3
"""
Firebase Storage Uploader

Upload files to Firebase Storage and get public download URLs.

Usage:
    python firebase_upload.py --env /path/to/.env --source /path/to/file.zip --dest folder/file.zip

Required .env variables:
    FB_PROJECT_ID
    FB_PRIVATE_KEY
    FB_CLIENT_EMAIL
    FB_STORAGE_BUCKET
"""

import argparse
import os
import sys
from pathlib import Path

def load_env(env_path: str) -> dict:
    """Load environment variables from .env file."""
    env_vars = {}
    with open(env_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                env_vars[key.strip()] = value.strip()
    return env_vars

def get_firebase_cred(env_vars: dict) -> dict:
    """Build Firebase credential dict from env vars."""
    private_key = env_vars.get('FB_PRIVATE_KEY', '')
    # Handle escaped newlines
    private_key = private_key.replace('\\n', '\n')
    
    return {
        "type": "service_account",
        "project_id": env_vars.get('FB_PROJECT_ID', ''),
        "private_key_id": env_vars.get('FB_PROJECT_KEY_ID', ''),
        "private_key": private_key,
        "client_email": env_vars.get('FB_CLIENT_EMAIL', ''),
        "client_id": env_vars.get('FB_CLIENT_ID', ''),
        "auth_uri": env_vars.get('FB_AUTH_URI', 'https://accounts.google.com/o/oauth2/auth'),
        "token_uri": env_vars.get('FB_TOKEN_URI', 'https://oauth2.googleapis.com/token'),
        "auth_provider_x509_cert_url": env_vars.get('FB_AUTH_PROVIDER_X', 'https://www.googleapis.com/oauth2/v1/certs'),
        "client_x509_cert_url": env_vars.get('FB_CLIENT_X', ''),
        "universe_domain": env_vars.get('FB_UNIVERSE_DOMAIN', 'googleapis.com')
    }

def upload_to_firebase(env_path: str, source_path: str, dest_path: str, make_public: bool = True) -> str:
    """
    Upload a file to Firebase Storage.
    
    Args:
        env_path: Path to .env file with Firebase credentials
        source_path: Local file path to upload
        dest_path: Destination path in Firebase Storage (e.g., 'folder/file.zip')
        make_public: Whether to make the file publicly accessible
        
    Returns:
        Public URL or signed URL of the uploaded file
    """
    try:
        import firebase_admin
        from firebase_admin import credentials, storage
    except ImportError:
        print("Error: firebase-admin not installed. Run: pip install firebase-admin")
        sys.exit(1)
    
    # Validate inputs
    if not os.path.exists(env_path):
        print(f"Error: .env file not found: {env_path}")
        sys.exit(1)
    
    if not os.path.exists(source_path):
        print(f"Error: Source file not found: {source_path}")
        sys.exit(1)
    
    # Load credentials
    print(f"Loading credentials from {env_path}...")
    env_vars = load_env(env_path)
    
    required_vars = ['FB_PROJECT_ID', 'FB_PRIVATE_KEY', 'FB_CLIENT_EMAIL', 'FB_STORAGE_BUCKET']
    missing = [v for v in required_vars if not env_vars.get(v)]
    if missing:
        print(f"Error: Missing required env vars: {', '.join(missing)}")
        sys.exit(1)
    
    # Initialize Firebase (only once)
    if not firebase_admin._apps:
        cred = credentials.Certificate(get_firebase_cred(env_vars))
        firebase_admin.initialize_app(cred, {
            'storageBucket': env_vars['FB_STORAGE_BUCKET']
        })
    
    # Upload file
    bucket = storage.bucket()
    blob = bucket.blob(dest_path)
    
    file_size = os.path.getsize(source_path)
    print(f"Uploading {source_path} ({file_size / 1024 / 1024:.2f} MB) to {dest_path}...")
    
    blob.upload_from_filename(source_path)
    print("Upload complete!")
    
    # Get URL
    if make_public:
        blob.make_public()
        url = blob.public_url
        print(f"\nPublic URL:\n{url}")
    else:
        from datetime import timedelta
        url = blob.generate_signed_url(expiration=timedelta(days=7))
        print(f"\nSigned URL (valid for 7 days):\n{url}")
    
    return url

def main():
    parser = argparse.ArgumentParser(
        description='Upload files to Firebase Storage',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Upload with public URL
    python firebase_upload.py --env .env --source file.zip --dest uploads/file.zip
    
    # Upload with signed URL (not public)
    python firebase_upload.py --env .env --source file.zip --dest uploads/file.zip --no-public
    
Required .env variables:
    FB_PROJECT_ID       - Firebase project ID
    FB_PRIVATE_KEY      - Service account private key
    FB_CLIENT_EMAIL     - Service account email
    FB_STORAGE_BUCKET   - Storage bucket name (e.g., my-project.appspot.com)
    
Optional .env variables:
    FB_PROJECT_KEY_ID   - Private key ID
    FB_CLIENT_ID        - Client ID
    FB_AUTH_URI         - Auth URI
    FB_TOKEN_URI        - Token URI
    FB_AUTH_PROVIDER_X  - Auth provider cert URL
    FB_CLIENT_X         - Client cert URL
    FB_UNIVERSE_DOMAIN  - Universe domain
        """
    )
    
    parser.add_argument('--env', '-e', required=True, help='Path to .env file')
    parser.add_argument('--source', '-s', required=True, help='Local file to upload')
    parser.add_argument('--dest', '-d', required=True, help='Destination path in Firebase Storage')
    parser.add_argument('--no-public', action='store_true', help='Generate signed URL instead of public URL')
    
    args = parser.parse_args()
    
    upload_to_firebase(
        env_path=args.env,
        source_path=args.source,
        dest_path=args.dest,
        make_public=not args.no_public
    )

if __name__ == '__main__':
    main()
