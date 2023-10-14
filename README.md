# Zip2S3Uploader
This is a Command-Line Interface (CLI) utility that accepts a URL pointing to a ZIP archive as an input argument. It efficiently processes and concurrently uploads the contents of the ZIP archive to Amazon S3, ensuring enhanced performance.

## Usage
To use the Zip to S3 Uploader, follow the instructions below:

### Installation
1. Clone this repository to your local machine.
2. Install the required dependencies using [Poetry](https://python-poetry.org/):
```
poetry install
```

### Command-Line Usage
Run the utility with the following command:

```
poetry run python main.py <zip_url> --bucket <bucket_name> [--concurrency <level>] [--verbose]
```

- **'<zip_url>'**: The URL of the ZIP archive to download.
- **'--bucket <bucket_name>'**: The name of the S3 bucket where the files will be uploaded.
- **'--concurrency <level>'**: (optional): Concurrency level (default is 1).
- **'--verbose'**:  (optional): Enable verbose output.

## Dependencies
- Python (>= 3.8)
- Boto3 (>= 1.28.63)
- Moto (>= 4.2.5) - Only required for running tests in a development environment.

### Note for Developers
This project relies on the Moto library for testing purposes. If you're a developer and want to run the tests, please make sure to install Moto with the specified version (>= 4.2.5) as indicated in the **'pyproject.toml'** file under the **'[tool.poetry.group.dev.dependencies]'** section.

## Running Tests
You can run the utility's tests to ensure its functionality. Use the following command:

```
poetry run python tests.py
```
Ensure that your development environment is properly set up for testing.

## Authors
- Roman [98953084+Rokuflam@users.noreply.github.com](98953084+Rokuflam@users.noreply.github.com)
  
