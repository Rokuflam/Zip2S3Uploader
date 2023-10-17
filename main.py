"""
CLI utility that as an argument receive URL to the ZIP archive and upload archives files to the S3 with concurrency.
authors: ["Roman <98953084+Rokuflam@users.noreply.github.com>"]
"""
import argparse
import boto3
import concurrent.futures
import requests
import os
import zipfile
from io import BytesIO

# Set AWS credentials via environment variables
AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID', None)
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY', None)
AWS_REGION_NAME = os.environ.get('AWS_REGION_NAME', None)


def download_zip(zip_url):
    """
    Download a ZIP archive from a given URL.

    Args:
        zip_url (str): The URL of the ZIP archive to download.

    Returns:
        BytesIO: A BytesIO object containing the ZIP archive content.
    """
    try:
        response = requests.get(zip_url)
        response.raise_for_status()
        return BytesIO(response.content)
    except requests.exceptions.RequestException as e:
        raise Exception(f"Failed to download ZIP archive from {zip_url}: {e}")


def extract_and_upload_file(zip_url, bucket_name, s3_client, concurrency_level=1):
    """
    This function extracts files from a provided ZIP archive and concurrently
    uploads them to an Amazon S3 bucket using multiple threads.
    Parameters:
    - zip_url (str): The URL of the ZIP archive to extract files from.
    - bucket_name (str): The name of the Amazon S3 bucket where files will be uploaded.
    - s3_client (boto3.client): The Amazon S3 client used for file uploads.
    - concurrency_level (int, optional): The number of concurrent file uploads. Default is 1.
    Behavior:
    - It first downloads the ZIP archive from the specified URL.
    - Then, it extracts files from the ZIP archive using the Python 'zipfile' library.
    - For concurrent uploads, it spawns a thread pool executor with the given concurrency level.
    - Each file extracted from the ZIP archive is submitted for uploading to the S3 bucket.
    - The function waits for all uploads to complete before returning.
    """
    zip_content = download_zip(zip_url)
    with zipfile.ZipFile(zip_content, 'r') as zip_file:
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency_level) as executor:
            futures = []
            for file_info in zip_file.infolist():
                if not file_info.filename.endswith('/'):
                    futures.append(executor.submit(upload_file, zip_file, file_info, bucket_name, s3_client))

            concurrent.futures.wait(futures)  # Wait for all uploads to complete


def upload_file(zip_file, file_info, bucket_name, s3_client):
    """
    Uploads a file from a ZIP archive to an S3 bucket.

    This function takes a ZIP archive, extracts a specific file defined by 'file_info,'
    and uploads it to the specified S3 bucket.

    Parameters:
    - zip_file (zipfile.ZipFile): The ZIP archive containing the file to be uploaded.
    - file_info (zipfile.ZipInfo): Information about the file to be uploaded.
    - bucket_name (str): The name of the S3 bucket where the file will be uploaded.
    - s3_client (boto3.client): An S3 client used for uploading files to the S3 bucket.

    Behavior:
    - Opens the specified file within the ZIP archive.
    - Uploads the file to the provided S3 bucket with the specified key.

    """
    with zip_file.open(file_info) as file:
        s3_key = file_info.filename
        s3_client.upload_fileobj(file, bucket_name, s3_key)


def main():
    """
    Main function for the ZIP to S3 uploader utility.

    This function parses command-line arguments, downloads a ZIP archive from a given URL,
    extracts and uploads its contents to an S3 bucket with optional concurrency, and provides
    verbose output when specified.

    Command-line Arguments:
        - zip_url (str): URL to the ZIP archive.
        - --bucket (str): S3 bucket name.
        - --concurrency (int, optional): Concurrency level. Default is 1.
        - --verbose (flag, optional): Enable verbose output.

    Usage:
        python main.py <zip_url> --bucket <bucket_name> [--concurrency <level>] [--verbose]

    Returns:
        None
    """
    parser = argparse.ArgumentParser(description="Utility to upload files from a ZIP archive to S3 with concurrency.")
    parser.add_argument("zip_url", help="URL to the ZIP archive")
    parser.add_argument("--bucket", help="S3 bucket name")
    parser.add_argument("--concurrency", type=int, default=1, help="Concurrency level")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output")
    args = parser.parse_args()

    s3_client = boto3.client(
        's3',
        region_name=AWS_REGION_NAME,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

    if args.verbose:
        print(f"Downloading ZIP archive from {args.zip_url}")

    extract_and_upload_file(args.zip_url, args.bucket, s3_client, args.concurrency)


if __name__ == "__main__":
    main()
