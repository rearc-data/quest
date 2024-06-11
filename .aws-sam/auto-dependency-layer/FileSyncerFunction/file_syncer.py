import urllib
import boto3
import os
import re
import logging
from datetime import datetime
from bs4 import BeautifulSoup
from botocore.client import Config
from urllib.request import urlopen


class FileSyncer:
    def __init__(self, host_url: str, s3_bucket: str) -> None:
        # Set up class variables for extract
        self.host_url = host_url

        # set up class variables for load
        self.prefix = "productivity_cost"
        self.files_pending_upload = []

        self.files_to_add = set()
        self.files_up_to_date = set()
        self.files_to_delete = set()
        
        s3 = boto3.resource('s3')
        self.bucket = s3.Bucket( "noventa-scratch-bucket")
        self.s3_bucket = s3_bucket
        self.s3_client = boto3.client('s3', 'us-east-2', config=Config(signature_version='s3v4'))

        # Configure URLLib to mimic a browser
        opener = urllib.request.build_opener()
        opener.addheaders = [("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")]
        urllib.request.install_opener(opener)

        # Create tmp directory if it doesn't exist
        if not os.path.exists("/tmp"):
            os.makedirs("/tmp")


    # Private helper functions for Syncer
    def _clean_up_local_file(self, filename: str) -> None:
        os.remove(f"/tmp/{filename}")


    def _check_files_to_update(self) -> None:
        logging.log(logging.INFO, f"Checking files to be updated...")

        s3_files = set()
        for obj in self.bucket.objects.filter(Prefix="productivity_cost/"):
            _, existing_file_date, existing_file = obj.key.split('/')
            s3_files.add((existing_file_date, existing_file))

        files_pending_upload = set(self.files_pending_upload)
        self.files_to_add = files_pending_upload - s3_files
        self.files_up_to_date = files_pending_upload & s3_files
        self.files_to_delete = s3_files - files_pending_upload

        # If an updated file is in files_to_add, remove it from files_up_to_date and add it to files_to_delete
        for add_date, add_file in self.files_to_add:
            for update_date, update_file in self.files_up_to_date.copy():
                if add_file == update_file:
                    self.files_up_to_date.remove((update_date, update_file))
                    self.files_to_delete.add((update_date, update_file))

        logging.log(logging.INFO, f"Staging the following files to upload: {self.files_to_add}")
        logging.log(logging.INFO, f"Staging the following files to delete: {self.files_to_delete}")
        logging.log(logging.INFO, f"The following files are up to date: {self.files_up_to_date}")


    def _create_date_directory(self, date: str) -> None:
        # Create tmp directory if it doesn't exist
        if not os.path.exists(f"/tmp/{date}"):
            logging.log(logging.INFO, f"Creating directory for date: {date}")
            os.makedirs(f"/tmp/{date}")

    # Public functions for Syncer
    def generate_presigned_urls(self, expires_in: int=604800) -> list[str]:
        available_files = self.files_up_to_date.union(self.files_to_add)

        presigned_urls = []
        for date, file in available_files:
            key = f"{self.prefix}/{date}/{file}"
            logging.log(logging.INFO, f"Generating presigned URL for key: {key}")
            presigned_urls.append(self.s3_client.generate_presigned_url('get_object',
                                            Params={'Bucket': self.s3_bucket,
                                                    'Key': f"{key}"},
                                            ExpiresIn=expires_in))           

        return presigned_urls            


    def extract_productivity_cost_data(self) -> None:
        # get latest file list
        logging.log(logging.INFO, f"Extracting productivity cost data from {self.host_url}")
        with urlopen(f"{self.host_url}/pub/time.series/pr/") as response:
            body = response.read()
            soup = BeautifulSoup(body, 'html.parser')

            dates = []
            for date in re.findall("[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}", str(soup.find_all('pre')[0])):
                dates.append(datetime.strptime(date.strip(), '%m/%d/%Y').strftime('%Y-%m-%d'))

            SKIP_FIRST_INDEX = slice(1, None, None)
            file_index = 0
            for link in soup.find_all('a')[SKIP_FIRST_INDEX]:        
                file_date = dates[file_index]
                file_index += 1

                file_name = link.get('href').split("/")[-1]

                self._create_date_directory(file_date)

                urllib.request.urlretrieve (f"{self.host_url}{link.get('href')}", f"/tmp/{file_date}/{file_name}")
                logging.log(logging.INFO, f"Downloaded file: {file_date}/{file_name}")
                self.files_pending_upload.append((file_date, file_name))


    def load_productivity_cost_data(self) -> None:
        self._check_files_to_update()

        logging.log(logging.INFO, f"Adding the following new files: {self.files_to_add}")
        for file_date, file_name in self.files_to_add:
            object_key = f"{self.prefix}/{file_date}/{file_name}"
            self.bucket.upload_file(f'/tmp/{file_date}/{file_name}', f"{object_key}")

        logging.log(logging.INFO, f"Deleting the following files: {self.files_to_delete}")
        for file_date, file_name in self.files_to_delete:
            object_key = f"{self.prefix}/{file_date}/{file_name}"
            self.s3_client.delete_objects(f's3://{self.s3_bucket}/{object_key}')


    def clean_up_local_files(self) -> None:
        for file_date, file_name in self.files_pending_upload:
            logging.log(logging.INFO, f"Cleaning up local file: {file_date}/{file_name}")
            self._clean_up_local_file(f"{file_date}/{file_name}")


    def purge_local_directory(self) -> None:
        for date in os.listdir("/tmp"):
            logging.log(logging.INFO, f"Removing local directory: {date}")
            os.rmdir(f"/tmp/{date}")