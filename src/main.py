import supervisely as sly
from supervisely.app.widgets import Container

import src.ui.connect as connect
import src.ui.team_selector as team_selector
import src.ui.entity_selector as entity_selector

main_container = Container(widgets=[connect.card, team_selector.card, entity_selector.card])
layout = main_container
app = sly.Application(layout=layout)
