import supervisely as sly
from supervisely.app.widgets import Container

import src.ui.connect as connect
import src.ui.team_selector as team_selector
import src.ui.entity_selector as entity_selector
from src.autorestart import AutoRestartInfo, autorestart, sync_on_autorestart

main_container = Container(widgets=[connect.card, team_selector.card, entity_selector.card])
layout = main_container
app = sly.Application(layout=layout)
AutoRestartInfo.check_autorestart(entity_selector.g.dst_api_task, entity_selector.g.task_id)
if autorestart is not None and sync_on_autorestart:
    try:
        sly.logger.info("Autorestart detected, applying deploy params...", extra=autorestart.deploy_params)
        entity_selector.process_import_from_autorestart(autorestart)
    except Exception as e:
        sly.logger.warning("Autorestart failed. Runnning app in normal mode.", exc_info=True)