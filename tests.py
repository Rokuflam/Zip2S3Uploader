"""
Download/Upload zip/txt files tests.
authors: ["Roman <98953084+Rokuflam@users.noreply.github.com>"]
"""
import boto3
import unittest
import zipfile
from io import BytesIO, StringIO
from unittest import mock
from moto import mock_s3

import main as zip_2_s3_uploader


class TestZipUploaderWithUnitTestMock(unittest.TestCase):

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


class TestZipUploaderWithMoto(unittest.TestCase):
    @mock_s3
    def setUp(self):
        # Initialize a mock S3 client
        self.s3_client = boto3.client('s3')
        self.bucket_name = 'test-bucket'
        self.s3_client.create_bucket(Bucket=self.bucket_name)

    @mock.patch('zipfile.ZipFile')
    @mock.patch('main.upload_file')
    def test_upload_file(self, mock_upload_file_to_s3, mock_zipfile):
        """
        Test the upload of a file from a temporary ZIP archive to an S3 bucket.

        This test creates a temporary ZIP archive with a test file, mocks the ZipFile
        constructor to return the temporary ZIP file, and calls the upload_file function.
        It verifies that the upload_file function calls the upload_file function as expected.
        """
        # Create a temporary ZIP file with a test file
        zip_content = BytesIO()
        with zipfile.ZipFile(zip_content, 'w') as zip_file:
            zip_file.writestr('test-file.txt', b'Test Content')

        # Mock the ZipFile constructor to return the temporary ZIP file
        mock_zipfile.return_value = zipfile.ZipFile(zip_content, 'r')

        # Call the upload_file function
        zip_2_s3_uploader.upload_file(zip_content, 'test-file.txt', self.bucket_name, self.s3_client)

        # Verify that upload_file_to_s3 was called with the expected arguments
        expected_calls = [
            mock.call(zip_content.__enter__(), 'test-file.txt', self.bucket_name, self.s3_client)
        ]
        mock_upload_file_to_s3.assert_has_calls(expected_calls)

    @mock.patch('main.download_zip')
    def test_extract_and_upload_file(self, mock_download_zip):
        """
        Test the extraction and upload of a file from a ZIP archive to an S3 bucket.

        This test simulates the extraction and upload of a file from a temporary ZIP archive.
        It mocks the download_zip function to avoid real HTTP requests and utilizes a Moto S3 mock
        to create an S3 bucket for testing. The test verifies that the file was successfully uploaded
        to the S3 bucket.
        """
        # Create a temporary ZIP file
        zip_content = BytesIO()
        with zipfile.ZipFile(zip_content, 'w') as zip_file:
            zip_file.writestr('test-file.txt', b'Test Content')

        # Mock the download_zip function to avoid real HTTP requests
        mock_download_zip.return_value = zip_content

        # Start a Moto S3 mock
        with mock_s3():
            # Create the S3 bucket
            self.s3_client.create_bucket(Bucket=self.bucket_name)

            # Test the extract_and_upload_file function
            zip_2_s3_uploader.extract_and_upload_file('http://example.com/test.zip', self.bucket_name, self.s3_client)

            # Verify that the file was uploaded to S3
            objects = self.s3_client.list_objects(Bucket=self.bucket_name)
            self.assertEqual(len(objects.get('Contents', [])), 1)


if __name__ == '__main__':
    unittest.main()
