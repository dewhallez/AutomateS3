import argparse
import json
import logging
import mimetypes
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Optional

try:
    # optional local secrets file — keep this optional so we prefer environment/profile credentials
    from secrets import access_key, secret_access_key  # type: ignore
except Exception:
    access_key = secret_access_key = None

import boto3
from botocore.exceptions import BotoCoreError, ClientError


def get_s3_client(access_key: Optional[str] = None, secret: Optional[str] = None):
    """Return a boto3 S3 client. If access_key/secret provided, use them; otherwise rely on default chain."""
    if access_key and secret:
        return boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret)
    return boto3.client('s3')


def upload_file(client, file_path: Path, bucket: str, dest_key: str):
    """Upload a single file to S3 to the explicit destination key.

    dest_key must be the full S3 key (including any prefix and path).
    """
    extra_args = {}
    mime_type, _ = mimetypes.guess_type(str(file_path))
    if mime_type:
        extra_args['ContentType'] = mime_type

    try:
        client.upload_file(str(file_path), bucket, dest_key, ExtraArgs=extra_args if extra_args else None)
        logging.info("Uploaded %s -> s3://%s/%s", file_path, bucket, dest_key)
    except (BotoCoreError, ClientError) as exc:
        logging.exception("Failed to upload %s: %s", file_path, exc)


def main(bucket: str, directory: str = '.', dry_run: bool = False, recursive: bool = False,
         mapping: Optional[Dict[str, str]] = None, workers: int = 1):
    client = get_s3_client(access_key, secret_access_key)
    base = Path(directory)

    if not base.exists():
        logging.error("Directory does not exist: %s", directory)
        return

    exclude_dirs = {"env", "__pycache__"}
    my_name = Path(__file__).name

    # gather files to upload
    files_to_upload = []  # list of tuples (Path, dest_key)

    # walk either recursively or only the top-level
    iterator = base.rglob('*') if recursive else base.iterdir()

    for p in iterator:
        # skip excluded directories and non-files
        if p.name in exclude_dirs and p.is_dir():
            continue
        if not p.is_file():
            continue
            if p.name == my_name:
                logging.debug("Skipping self file: %s", p)
                continue

            prefix = determine_prefix(p, mapping=mapping)
            if not prefix:
                logging.debug("Skipping unsupported file type: %s", p)
                continue

            # compute destination key; preserve relative path when recursive
            try:
                rel = p.relative_to(base)
            except Exception:
                # fallback to filename only
                rel = Path(p.name)

            dest_key = f"{prefix.rstrip('/')}/{rel.as_posix()}"
            files_to_upload.append((p, dest_key))

    # perform uploads, possibly concurrently
    if dry_run:
        for p, dest in files_to_upload:
            logging.info("[dry-run] would upload %s -> s3://%s/%s", p, bucket, dest)
        return

    if workers <= 1:
        for p, dest in files_to_upload:
            upload_file(client, p, bucket, dest)
    else:
        with ThreadPoolExecutor(max_workers=workers) as exc:
            futures = {exc.submit(upload_file, client, p, bucket, dest): (p, dest) for p, dest in files_to_upload}
            for fut in as_completed(futures):
                p, dest = futures[fut]
                try:
                    fut.result()
                except Exception:
                    logging.exception("Upload task failed for %s -> %s", p, dest)


def determine_prefix(file_path: Path, mapping: Optional[Dict[str, str]] = None) -> Optional[str]:
    """Return the S3 key prefix for a given file path, or None to skip.

    Rules:
    - .py -> 'python'
    - .jpg, .jpeg, .png -> 'pictures'
    - otherwise -> None
    """
    suffix = file_path.suffix.lower()
    if mapping:
        # mapping keys may be provided with or without leading dot
        key = suffix.lower()
        if key in mapping:
            return mapping[key]
        key_nodot = key.lstrip('.')
        if key_nodot in mapping:
            return mapping[key_nodot]

    # built-in defaults
    if suffix in ('.py',):
        return 'python'
    if suffix in ('.jpg', '.jpeg', '.png'):
        return 'pictures'
    return None


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Upload selected files from a directory to S3')
    parser.add_argument('--bucket', '-b', default='my-s3-uploader-test', help='S3 bucket name')
    parser.add_argument('--dir', '-d', default='.', help='Directory to scan for files')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be uploaded without performing uploads')
    parser.add_argument('--recursive', '-r', action='store_true', help='Recurse into subdirectories')
    parser.add_argument('--map', '-m', dest='map', help='Path to JSON file mapping extensions to prefixes (e.g. {".py": "python"})')
    parser.add_argument('--workers', '-w', type=int, default=1, help='Number of concurrent upload workers')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    # load mapping if provided
    mapping = None
    if getattr(args, 'map', None):
        try:
            with open(args.map, 'r') as fh:
                mapping = json.load(fh)
        except Exception as exc:
            logging.error('Failed to load mapping file %s: %s', args.map, exc)
            raise SystemExit(1)

    main(args.bucket, args.dir, args.dry_run, args.recursive, mapping, args.workers)

