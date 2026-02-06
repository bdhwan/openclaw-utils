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
import mimetypes
import os
import sys
from pathlib import Path

# Content type mappings with UTF-8 charset for text files
CONTENT_TYPES = {
    '.md': 'text/markdown; charset=utf-8',
    '.txt': 'text/plain; charset=utf-8',
    '.html': 'text/html; charset=utf-8',
    '.htm': 'text/html; charset=utf-8',
    '.css': 'text/css; charset=utf-8',
    '.js': 'application/javascript; charset=utf-8',
    '.json': 'application/json; charset=utf-8',
    '.xml': 'application/xml; charset=utf-8',
    '.csv': 'text/csv; charset=utf-8',
    '.yaml': 'text/yaml; charset=utf-8',
    '.yml': 'text/yaml; charset=utf-8',
}

def get_content_type(filename: str) -> str:
    """Get content type for a file, with UTF-8 charset for text files."""
    ext = Path(filename).suffix.lower()
    
    # Check our custom mappings first (with charset)
    if ext in CONTENT_TYPES:
        return CONTENT_TYPES[ext]
    
    # Fall back to mimetypes
    content_type, _ = mimetypes.guess_type(filename)
    if content_type:
        # Add charset for text types
        if content_type.startswith('text/'):
            return f"{content_type}; charset=utf-8"
        return content_type
    
    # Default
    return 'application/octet-stream'

def load_env(env_path: str) -> dict:
    """Load environment variables from .env file."""
    env_vars = {}
    with open(env_path, 'r', encoding='utf-8') as f:
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

def upload_to_firebase(env_path: str, source_path: str, dest_path: str, make_public: bool = True, content_type: str = None) -> str:
    """
    Upload a file to Firebase Storage.
    
    Args:
        env_path: Path to .env file with Firebase credentials
        source_path: Local file path to upload
        dest_path: Destination path in Firebase Storage (e.g., 'folder/file.zip')
        make_public: Whether to make the file publicly accessible
        content_type: Override content type (auto-detected if not specified)
        
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
    
    # Determine content type
    if content_type is None:
        content_type = get_content_type(source_path)
    
    # Upload file
    bucket = storage.bucket()
    blob = bucket.blob(dest_path)
    
    file_size = os.path.getsize(source_path)
    print(f"Uploading {source_path} ({file_size / 1024 / 1024:.2f} MB) to {dest_path}...")
    print(f"Content-Type: {content_type}")
    
    blob.upload_from_filename(source_path, content_type=content_type)
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
    
    # Upload with custom content type
    python firebase_upload.py --env .env --source data.bin --dest uploads/data.bin --content-type application/octet-stream
    
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
    parser.add_argument('--content-type', '-c', help='Override content type (auto-detected by default)')
    
    args = parser.parse_args()
    
    upload_to_firebase(
        env_path=args.env,
        source_path=args.source,
        dest_path=args.dest,
        make_public=not args.no_public,
        content_type=args.content_type
    )

if __name__ == '__main__':
    main()
