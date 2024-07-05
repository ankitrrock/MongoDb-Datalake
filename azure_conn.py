import json
import time
from pathlib import Path
from typing import Union

from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobClient, BlobServiceClient, StorageStreamDownloader

class NotJsonFile(Exception):
    def __init__(self, file_path, extension) -> None:
        self.file_path = file_path
        self.extension = extension
        self.message = "file must be a json file"
        super().__init__(self.message)


class CheckJsonFileType:
    def __init__(self, file_path):
        self.file_path = file_path
        self.check_file_type()

    def check_file_type(self):
        ext = Path(self.file_path).suffix
        if ext != ".json":
            raise NotJsonFile(file_path=self.file_path, extension=ext)
        else:
            return True


class BlobOperation:
    """
    class to perform operations on azure blob storage.
    """

    def __init__(
        self, container_name: str, blob_account_url: str, blob_credential: str
    ):
        """
        class to perform operations on azure blob storage.

        Args:
            container_name `str`: name of the container on which operation need to be performed.
            blob_account_url `str`: account url
            blob_credential `str`: account credential
        """
        self.container_name: str = container_name
        self.blob_account_url: str = blob_account_url
        self.blob_credential: str = blob_credential

    def create_connection(self) -> BlobServiceClient:
        """
        method to create blob service connection.

        Returns:
            `BlobServiceClient`: BlobServiceClient
        """
        conn: BlobServiceClient = BlobServiceClient(
            account_url=self.blob_account_url, credential=self.blob_credential
        )
        return conn

    def get_blob_connection(self, blob_path: str) -> BlobClient:
        """
        method to create blob client connection.

        Args:
            blob_path `str`: create connection to container with specified blob path.

        Returns:
            `BlobClient`: BlobClient
        """
        conn: BlobServiceClient = self.create_connection()
        container: BlobClient = conn.get_blob_client(
            container=self.container_name, blob=blob_path
        )
        return container

    def download_json_file(self, blob_path: str) -> Union[list, dict, None]:
        """method to download a json file and load data into python object

        Args:
            blob_path `str`: blob location to download

        Returns:
            `Union[list,dict,None]`: convert data into python objects if error is encountered it returns None.
        """
        container: BlobClient = self.get_blob_connection(blob_path=blob_path)
        try:
            stream_data: StorageStreamDownloader = container.download_blob()
        except NotJsonFile as file_error:
            print("wrong file type")
            return None
        except ResourceNotFoundError as error_while_downloading_file:
            error_message = {
                "message": error_while_downloading_file.reason,
                "status_code": error_while_downloading_file.status_code,
                "blob_path": blob_path,
            }
            print(error_message)
            return None
        else:
            if isinstance(stream_data, StorageStreamDownloader):
                byte_data: bytes = stream_data.readall()
                json_data: Union[list, dict] = json.loads(byte_data)
                return json_data

    # Function to upload a single file
    def upload_data(self, filename, filelocation):
        # Set the name for your blob
        blob_name = filelocation
        # Get a BlobClient object to represent the blob
        container_client = self.create_connection().get_container_client(
            self.container_name
        )
        blob_client = container_client.get_blob_client(blob_name)
        # Specify the path to the file you want to upload
        file_path = filename
        # Upload the file to the blob
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data)

    def upload_jpeg_file(self, blob_path, local_file_path):
        """
        method to upload image file to azure blob storage.

        Args:
            blob_path `blob_storage_path`: azure blob location at which image will be uploaded
            local_file_path 'local_file_path': local location from where image will be uploaded
        """
        # Get a BlobClient object to represent the blob
        container_client = self.create_connection().get_container_client(
            self.container_name
        )
        blob_client = container_client.get_blob_client(blob_path)
        try:
            with open(file=local_file_path, mode="rb") as csv_file:
                response = blob_client.upload_blob(csv_file, overwrite=True)
            return response
        except Exception as e:
            print(e)
            return None

    def upload_json_file(self, data: Union[list, dict], blob_path: str):
        """
        method to upload json file to azure blob storage.

        Args:
            data `list | dict `: data must be of type list or dict
            blob_path `blob_storage_path`: location at which file will be created

        Raises:
            `TypeError`: rasie error if data is not of type list or dict
        """
        container: BlobClient = self.get_blob_connection(blob_path=blob_path)
        if isinstance(data, (list, dict)):
            container.upload_blob(data=json.dumps(data), overwrite=True)
        else:
            raise TypeError("data must be a list or dict")

    def upload_csv_file(self, blob_path, local_file_path):
        container: BlobClient = self.get_blob_connection(blob_path=blob_path)
        with open(file=local_file_path, mode="rb") as csv_file:
            file_put_response = container.upload_blob(csv_file, overwrite=True)
        return file_put_response

    def blob_file_location(account_id: str, type_: str, user_id: str):
        """
        returns file location for storing file in Azure Blob Storage
        """
        time_stamp = time.time()

        path_object = {
            "User_Posts": f"data_collection/facebook/{account_id}/userposts/{time_stamp}.json",
            "Page_Posts": f"data_collection/facebook/{account_id}/pageposts/{time_stamp}.json",
            "User_Posts_Csv": f"media_collection/facebook/{account_id}/userposts/{time_stamp}.csv",
            "Page_Posts_Csv": f"media_collection/facebook_page/{account_id}/posts/facebook_page_posts/{time_stamp}.csv",
        }

        return path_object.get(type_, None)


def blob_file_location(type_: str, user_id: str):
    """
    returns file location for storing file in Azure blob based on the type of file
    """

    path_object = {
        "profile_picture_path_azure": f"collected_media/facebook_user/profiles/{user_id}.jpg",
        "page_picture_path": f"collected_media/facebook_page/profiles/{user_id}.jpg",
        "page_cover_path": f"collected_media/facebook_page/covers/{user_id}.jpg",
        "profile_cover_picture_path_azure": f"collected_media/facebook_user/covers/{user_id}.jpg",
    }

    return path_object.get(type_, None)