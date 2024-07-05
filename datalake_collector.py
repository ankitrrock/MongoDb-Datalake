from functools import wraps
import json
from azure.core.exceptions import AzureError
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeFileClient
from facebook.utils.logs.logger_info import error_logger
import time
from pathlib import Path
from typing import Dict, List, Union


def exception_logging_decorator_for_class_method(method):
    @wraps(method)
    def wrapper_function(self, *args, **kwargs):
        try:
            response = method(self, *args, **kwargs)
        except Exception as e:
            error_logger.error(e)
        else:
            return response

    return wrapper_function


class ExceptionLoggingMetaClass(type):
    def __new__(
        cls,
        name_of_class_implement_meta_class,
        name_of_class_that_inhertic_other_class,
        class_attributes_of_class_implement_meta_class_and_it_should_be_dict,
    ):
        for (
            attr_name,
            attr_value,
        ) in (
            class_attributes_of_class_implement_meta_class_and_it_should_be_dict.items()
        ):
            if not attr_name.startswith("__") and callable(attr_value):
                class_attributes_of_class_implement_meta_class_and_it_should_be_dict[
                    attr_name
                ] = exception_logging_decorator_for_class_method(attr_value)

        return super().__new__(
            cls,
            name_of_class_implement_meta_class,
            name_of_class_that_inhertic_other_class,
            class_attributes_of_class_implement_meta_class_and_it_should_be_dict,
        )


class DataLakeOperation(metaclass=ExceptionLoggingMetaClass):
    def __init__(
        self,
        *,
        account_url,
        file_system_name_or_container_name,
        client_secret,
        report_id,
        tenant_id,
        client_id,
        secure_profile_handle_id,
        profile_id,
        platform_name
    ):
        self.account_url = account_url
        self.file_system_name_or_container_name = file_system_name_or_container_name
        self.client_secret = client_secret
        self.report_id = report_id
        self.secure_profile_handle_id = secure_profile_handle_id
        self.profile_id = profile_id
        self.platform_name = platform_name
        self.tenant_id = tenant_id
        self.client_id = client_id

    def create_client_credential(self) -> ClientSecretCredential:
        cred: ClientSecretCredential = ClientSecretCredential(
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
        )
        return cred

    def get_datalake_file_system_client(self, *, file_path) -> DataLakeFileClient:
        dl_client: DataLakeFileClient = DataLakeFileClient(
            account_url=self.account_url,
            file_system_name=self.file_system_name_or_container_name,
            file_path=file_path,
            credential=self.create_client_credential(),
        )
        return dl_client

    def upload_file(
        self,
        file_path: str,
        data: Union[List[Dict], Dict, bytes, str] = None,
        local_file_path: str = None,
        upload_and_remove_local_file: bool = False
    ):
        """
        Uploads a file to the Data Lake.

        Args:
            file_path (str): File path in the Data Lake.
            data (Union[List[Dict], Dict, bytes, str], optional): Data to upload. Defaults to None.
            local_file_path (str, optional): Local file path to upload. Defaults to None.
            upload_and_remove_local_file (bool, optional): Whether to upload and remove the local file. Defaults to False.
        """
        retry = 3

        while retry > 0:
            dl_client = self.get_datalake_file_system_client(file_path=file_path)

            try:
                if local_file_path:
                    with open(local_file_path, mode="rb") as file_obj:
                        file_data = file_obj.read()
                    response = dl_client.upload_data(data=file_data, overwrite=True)
                elif data:
                    response = dl_client.upload_data(data=json.dumps(data, default=str), overwrite=True)
                else:
                    raise ValueError("Either 'local_file_path' or 'data' must be provided.")

                return response

            except FileNotFoundError as file_error:
                raise FileNotFoundError(f"The file path {local_file_path} does not exist in the local system.")
            except AzureError as er:
                if retry > 1 and er.status_code == 412:
                    error_logger.error(f"Error while uploading file to datalake error_code: {er.status_code}, message: {er.reason}, file_path: {file_path}")
                else:
                    break

            retry -= 1
            time.sleep(5)

        if retry == 0:
            raise Exception({"message": er.reason, "status_code": er.status_code, "file_system_name": self.file_system_name_or_container_name, "file_path": file_path})

        if upload_and_remove_local_file and local_file_path:
            file_to_remove = Path(local_file_path)
            if file_to_remove.exists():
                file_to_remove.unlink()

