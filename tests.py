"""
Download/Upload zip/txt files tests.
authors: ["Roman <98953084+Rokuflam@users.noreply.github.com>"]
"""
import boto3
import tempfile
import unittest
import zipfile
from io import BytesIO, StringIO
from unittest import mock
from moto import mock_s3

import main as zip_2_s3_uploader


class TestZipUploaderWithMoto(unittest.TestCase):

    @mock.patch('requests.get')
    def test_download_zip_success(self, mock_get):
        """
        Test the successful download of a ZIP archive.

        This test mocks a successful download response, ensuring that the `download_zip`
        function correctly retrieves and returns the content of a ZIP archive.
        """
        # Mock a successful download response
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.content = b'Test ZIP Content'
        mock_get.return_value = mock_response

        zip_url = 'http://example.com/test.zip'
        zip_content = zip_2_s3_uploader.download_zip(zip_url)
        self.assertEqual(zip_content.read(), b'Test ZIP Content')

    @mock.patch('requests.get')
    def test_download_zip_failure(self, mock_get):
        """
        Test the failure scenario of downloading a ZIP archive.

        This test mocks a failed download response, ensuring that the `download_zip`
        function raises an exception when a non-200 HTTP status code is encountered.
        """
        # Mock a failed download response
        mock_response = mock.Mock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        zip_url = 'http://example.com/nonexistent.zip'
        with self.assertRaises(Exception):
            zip_2_s3_uploader.download_zip(zip_url)

    @mock_s3
    def test_upload_file_to_s3(self):
        """
        Test the upload of a file to an S3 bucket.

        This test starts a mock S3 service and uploads a temporary file to a test S3 bucket
        using the `upload_file_to_s3` function. It verifies that the correct arguments are passed
        to the S3 client's `upload_fileobj` method.
        """
        # Start the mock S3 service
        conn = boto3.client('s3', region_name='us-east-1')
        bucket_name = 'test-bucket'
        conn.create_bucket(Bucket=bucket_name)
        file_name = 'test-file.txt'

        # Create a temporary file to upload
        with tempfile.TemporaryFile(delete=False) as temp_file:
            temp_file.write(b'Test Content')

        # Mock the boto3.client('s3') to capture the arguments
        with mock.patch('boto3.client') as mock_client:
            mock_s3_client = mock.Mock()
            mock_client.return_value = mock_s3_client
            zip_2_s3_uploader.upload_file_to_s3(temp_file.name, file_name, bucket_name, mock_s3_client)

            # Ensure that the upload_fileobj was called with the expected arguments
            mock_s3_client.upload_fileobj.assert_called_with(
                mock.ANY,  # Ensure it's called with a file-like object
                bucket_name,
                file_name
            )

    @mock_s3
    def test_extract_and_upload_file(self):
        """
        Test the extraction and upload of files from a ZIP archive to an S3 bucket.

        This test starts a mock S3 service, creates a temporary ZIP file with sample contents,
        and tests the extraction and upload process using the `extract_and_upload_file` function.
        It verifies that the extracted files are correctly uploaded to the S3 bucket.
        """
        # Start the mock S3 service
        conn = boto3.client('s3', region_name='us-east-1')
        bucket_name = 'test-bucket'
        conn.create_bucket(Bucket=bucket_name)

        # Create a temporary ZIP file for testing
        with tempfile.NamedTemporaryFile(delete=False) as temp_zip:
            with zipfile.ZipFile(temp_zip.name, 'w') as zip_file:
                zip_file.writestr('file1.txt', b'Test Content 1')
                zip_file.writestr('file2.txt', b'Test Content 2')

        # Call the function to extract and upload files
        zip_2_s3_uploader.extract_and_upload_file(temp_zip.name, bucket_name, conn)

        # Verify that the files were uploaded to the S3 bucket
        objects = conn.list_objects_v2(Bucket=bucket_name)

        # Check if 'file1.txt' and 'file2.txt' exist in the S3 bucket
        expected_files = {'file1.txt', 'file2.txt'}
        uploaded_files = {obj['Key'] for obj in objects.get('Contents', [])}
        self.assertEqual(uploaded_files, expected_files)

    @mock_s3
    def test_extract_and_upload_file_with_directory_structure(self):
        """
        Test the extraction and upload of files with a directory structure from a ZIP archive to an S3 bucket.

        This test starts a mock S3 service,creates a temporary ZIP file with directory structure,
        and tests the extraction and upload process using the `extract_and_upload_file` function.
        It verifies that files with directory structure are correctly uploaded to the S3 bucket.
        """
        # Start the mock S3 service
        conn = boto3.client('s3', region_name='us-east-1')
        bucket_name = 'test-bucket'
        conn.create_bucket(Bucket=bucket_name)

        # Create a temporary ZIP file with a directory structure for testing
        with tempfile.NamedTemporaryFile(delete=False) as temp_zip:
            with zipfile.ZipFile(temp_zip.name, 'w') as zip_file:
                zip_file.writestr('folder/file1.txt', b'Test Content 1')
                zip_file.writestr('folder/file2.txt', b'Test Content 2')

        # Call the function to extract and upload files
        zip_2_s3_uploader.extract_and_upload_file(temp_zip.name, bucket_name, conn)

        # Verify that the files were uploaded to the S3 bucket
        objects = conn.list_objects_v2(Bucket=bucket_name)

        # Check if 'folder/file1.txt' and 'folder/file2.txt' exist in the S3 bucket
        expected_files = {'folder/file1.txt', 'folder/file2.txt'}
        uploaded_files = {obj['Key'] for obj in objects.get('Contents', [])}
        self.assertEqual(uploaded_files, expected_files)

    @mock.patch('main.download_zip')
    @mock.patch('main.extract_and_upload_file')
    @mock.patch('main.boto3.client')
    def test_main(self, mock_boto_client, mock_extract, mock_download):
        """
        Test the main function for ZIP to S3 upload utility.

        This test sets up mock data for the `main` function and simulates
        command-line arguments to test the entire process.
        It verifies that the main function processes a ZIP
        archive by downloading, extracting, and uploading it to an S3 bucket.
        """
        # Mock the boto3 client and set up some mock data
        mock_client = mock_boto_client('s3')
        mock_client.create_bucket(Bucket='test-bucket')

        # Set up mock responses for download_zip
        mock_download.return_value = BytesIO(b'Test ZIP Content')

        # Set up expected S3 upload calls (if any) for extract_and_upload_file
        mock_extract.side_effect = None  # Replace with expected behavior or exceptions

        # Capture standard output to check printed messages
        expected_output = ("Downloading ZIP archive from https://example.com\n"
                           "Extracting and uploading files to S3\n"
                           "Processing with 2 concurrency level\n")
        captured_output = StringIO()

        with mock.patch('sys.stdout', new=captured_output):
            # Simulate command-line arguments
            args = ['https://example.com', '--bucket', 'test-bucket', '--concurrency', '2', '--verbose']
            with mock.patch('sys.argv', ['tests.py'] + args):
                zip_2_s3_uploader.main()

        # Check if the printed output matches the expected output
        self.assertEqual(captured_output.getvalue(), expected_output)

    @mock.patch('main.download_zip')
    @mock.patch('main.extract_and_upload_file')
    @mock.patch('main.boto3.client')
    def test_main_with_no_verbose_output(self, mock_boto_client, mock_extract, mock_download):
        """
        Test the main function for ZIP to S3 upload utility without verbose output.

        This test is similar to the main function test,
        but it checks for the case where verbose output is not enabled.
        It ensures that the main function processes the ZIP archive and prints no verbose output.
        """
        # Mock the boto3 client and set up some mock data
        mock_client = mock_boto_client('s3')
        mock_client.create_bucket(Bucket='test-bucket')

        # Set up mock responses for download_zip
        mock_download.return_value = BytesIO(b'Test ZIP Content')

        # Set up expected S3 upload calls (if any) for extract_and_upload_file
        mock_extract.side_effect = None  # Replace with expected behavior or exceptions

        # Capture standard output to check printed messages
        expected_output = ""  # No verbose output expected
        captured_output = StringIO()

        with mock.patch('sys.stdout', new=captured_output):
            # Simulate command-line arguments
            args = ['https://example.com', '--bucket', 'test-bucket', '--concurrency', '2']
            with mock.patch('sys.argv', ['tests.py'] + args):
                zip_2_s3_uploader.main()

        # Check if the printed output matches the expected output
        self.assertEqual(captured_output.getvalue(), expected_output)


if __name__ == '__main__':
    unittest.main()
