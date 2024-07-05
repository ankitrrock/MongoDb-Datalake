from facebook.utils.config.config import Config
from facebook.utils.logs.logger_info import error_logger, info_logger
from facebook.utils.storage.datalake_collector import DataLakeOperation


class SaveDataToDatalake:
    def __init__(self) -> None:
        self.dl_obj = DataLakeOperation(
            account_url=Config.DATA_LAKE_ACCOUNT_URL,
            file_system_name_or_container_name=Config.DATA_LAKE_FILE_SYSTEM_NAME,
            report_id=1,
            secure_profile_handle_id=2,
            profile_id=1,
            client_secret=Config.DATA_LAKE_CLIENT_SECRET,
            tenant_id=Config.DATA_LAKE_TENANT_ID,
            client_id=Config.DATA_LAKE_CLIENT_ID,
            platform_name=None,
        )

    def save_data_to_datalake(self, filelocation, data, social_user_id=None):
        
        try:
            response = self.dl_obj.upload_file(file_path=filelocation, data=data)
            if response: 
                if response.get("etag"):
                    info_logger.info(
                        f"data stored in datalake successfully for account:{social_user_id}"
                    )
                    return True
                else:
                    error_logger.error(
                        f"datalake upload failed for account: {social_user_id}"
                    )
                    return False
        except Exception as e:
            error_logger.error(e)
            return False
