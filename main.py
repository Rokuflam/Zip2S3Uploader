"""
CLI utility that as an argument receive URL to the ZIP archive and upload archives files to the S3 with concurrency.
authors: ["Roman <98953084+Rokuflam@users.noreply.github.com>"]
"""
import argparse
import boto3
import concurrent.futures
import os
import requests
import tempfile
import zipfile
from io import BytesIO


# Set AWS credentials via environment variables
AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID', None)
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY', None)


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
        response.raise_for_status()  # Raise an exception for HTTP errors
        return BytesIO(response.content)
    except requests.exceptions.RequestException as e:
        raise Exception(f"Failed to download ZIP archive from {zip_url}: {e}")


def upload_file_to_s3(file_path, file_name, bucket_name, s3_client):
    """
    Upload a file to an S3 bucket.

    Args:
        file_path (str): The path to the local file to upload.
        file_name (str): The name to give the file in the S3 bucket.
        bucket_name (str): The name of the S3 bucket.
        s3_client: An initialized Boto3 S3 client.
    """
    with open(file_path, 'rb') as file:
        s3_client.upload_fileobj(file, bucket_name, file_name)


def extract_and_upload_file(zip_path, bucket_name, s3_client):
    """
    Extract and upload files from a ZIP archive to an S3 bucket.

    Args:
        zip_path (str): The path to the ZIP archive.
        bucket_name (str): The name of the S3 bucket.
        s3_client: An initialized Boto3 S3 client.
    """
    with zipfile.ZipFile(zip_path, 'r') as zip_file:
        for file_info in zip_file.infolist():
            if not file_info.filename.endswith('/'):
                with zip_file.open(file_info.filename) as file:
                    # Create a temporary file to hold the extracted content.
                    with tempfile.TemporaryFile(prefix=os.path.basename(file_info.filename), delete=False) as temp_file:
                        temp_file.write(file.read())
                    # Upload the temporary file to S3.
                    upload_file_to_s3(temp_file.name, file_info.filename, bucket_name, s3_client)
                    os.remove(temp_file.name)  # Clean up the temporary file.


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
    # Parse command-line arguments.
    parser = argparse.ArgumentParser(description="Utility to upload files from a ZIP archive to S3 with concurrency.")
    parser.add_argument("zip_url", help="URL to the ZIP archive")
    parser.add_argument("--bucket", help="S3 bucket name")
    parser.add_argument("--concurrency", type=int, default=1, help="Concurrency level")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output")
    args = parser.parse_args()

    # Initialize S3 client.
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

    # Download the ZIP archive from the provided URL.
    if args.verbose:
        print(f"Downloading ZIP archive from {args.zip_url}")

    zip_content = download_zip(args.zip_url)

    # Extract and upload files concurrently.
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        # Create a temporary file to hold the downloaded ZIP content.
        with tempfile.NamedTemporaryFile(delete=False) as zip_file:
            zip_file.write(zip_content.read())

        if args.verbose:
            print("Extracting and uploading files to S3")

        # Submit the extraction and upload task.
        executor.submit(extract_and_upload_file, zip_file.name, args.bucket, s3_client)

        if args.verbose:
            print(f"Processing with {args.concurrency} concurrency level")


if __name__ == "__main__":
    main()
