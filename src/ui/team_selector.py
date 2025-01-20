import pandas as pd
import supervisely as sly
from supervisely.app.widgets import Card, Container, Progress, Table

TEAM_ID = "id".upper()
TEAM_NAME = "name".upper()
WORKSPACES = "workspaces".upper()
TEAM_MEMBERS = "members".upper()
SELECT = "select".upper()

columns = [TEAM_ID, TEAM_NAME, SELECT]
lines = []
table = Table(per_page=5, page_sizes=[5, 10, 15, 30, 50, 100], width="70%")
table.hide()

teams_progress = Progress(hide_on_finish=False)
progress = Progress(hide_on_finish=True)

container = Container([table, progress])
card = Card(
    title="Select Source Team",
    description="List of teams available in the connected Supervisely instance",
    content=container,
    lock_message="Connect to Supervisely Instance",
)
card.lock()


def build_table(src_api: sly.Api):
    global table, lines
    table.hide()
    lines = []
    table.loading = True
    teams = src_api.team.get_list()
    with teams_progress(message="Fetching Teams", total=len(teams)) as pbar:
        teams_progress.show()
        for info in teams:
            lines.append(
                [
                    info.id,
                    info.name or "-",
                    Table.create_button(SELECT),
                ]
            )
            pbar.update()
    df = pd.DataFrame(lines, columns=columns)
    table.read_pandas(df)
    table.loading = False
    teams_progress.hide()
    table.show()
