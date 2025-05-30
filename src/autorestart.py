from typing import List
from supervisely import logger, Api
from supervisely.api.module_api import ApiField
from dataclasses import dataclass


autorestart = None
sync_on_autorestart = False

@dataclass
class AutoRestartInfo:
    deploy_params: dict
    

    class Fields:
        AUTO_RESTART_INFO = "autoRestartInfo"
        DEPLOY_PARAMS = "deployParams"

    def generate_fields(self) -> List[dict]:
        return [
            {
                ApiField.FIELD: self.Fields.AUTO_RESTART_INFO,
                ApiField.PAYLOAD: {self.Fields.DEPLOY_PARAMS: self.deploy_params},
            }
        ]

    @classmethod
    def from_response(cls, data: dict):
        autorestart_info = data.get(cls.Fields.AUTO_RESTART_INFO, None)
        if autorestart_info is None:
            return None
        return cls(deploy_params=autorestart_info.get(cls.Fields.DEPLOY_PARAMS, None))

    def is_changed(self, deploy_params: dict) -> bool:
        return self.deploy_params != deploy_params
    
    @staticmethod
    def check_autorestart(api: Api , task_id: int) -> None:
        global autorestart, sync_on_autorestart
        try:
            if task_id is not None:
                logger.debug("Checking autorestart info...")
                response = api.task.get_fields(
                    task_id, [AutoRestartInfo.Fields.AUTO_RESTART_INFO]
                )
                autorestart = AutoRestartInfo.from_response(response)
                if autorestart is not None:
                    logger.info("Autorestart info:", extra=autorestart.deploy_params)
                    sync_on_autorestart = True
                else:
                    logger.debug("Autorestart info is not set.")
        except Exception:
            logger.error("Autorestart info is not available.", exc_info=True)