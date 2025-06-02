import supervisely as sly
from supervisely.app.widgets import Container
import threading
import asyncio
import src.ui.connect as connect
import src.ui.team_selector as team_selector
import src.ui.entity_selector as entity_selector
from src.autorestart import AutoRestartInfo


def safe_check_autorestart():
    """ Safely checks for autorestart information and processes it if available.
    This function is designed to handle exceptions gracefully and log any issues that arise during the check.
    It ensures that the autorestart check does not disrupt the normal operation of the application.
    """
    try:
        if not hasattr(entity_selector, 'g') or entity_selector.g.dst_api_task is None:
            sly.logger.warning("Entity selector not initialized, skipping autorestart check")
            return
            
        autorestart =  AutoRestartInfo.check_autorestart(entity_selector.g.dst_api_task, entity_selector.g.task_id)
        sly.logger.debug("Autorestart info checked")
        if autorestart is not None and autorestart.deploy_params.get("autorestart"):
            try:
                log_params = {item: value for item, value in autorestart.deploy_params.items() if item != "src_token"}
                sly.logger.info("Autorestart detected, applying deploy params: ", extra=log_params)
                entity_selector.process_import_from_autorestart(autorestart)
            except Exception as e:
                sly.logger.warning("Autorestart failed. Runnning app in normal mode.", exc_info=True)
    except Exception as e:
        sly.logger.error("Error in autorestart check", exc_info=True)

async def startup_event():
    sly.logger.info("Server startup completed, triggering autorestart check...")
    try:
        await asyncio.sleep(5)
        threading.Thread(target=safe_check_autorestart, daemon=True).start()
    except Exception as e:
        sly.logger.error("Failed to check autorestart ", exc_info=True)

main_container = Container(widgets=[connect.card, team_selector.card, entity_selector.card])
layout = main_container
app = sly.Application(layout=layout)
server = app.get_server()

server.add_event_handler("startup", startup_event)
