import os
import supervisely as sly
from dotenv import load_dotenv

if sly.is_development():
    load_dotenv(os.path.expanduser("~/supervisely.env"))
    load_dotenv("local.env")

dst_api: sly.Api = sly.Api()
# remove x-task-id header
dst_api.headers.pop("x-task-id", None)

src_api: sly.Api = None
team_id = sly.env.team_id()

boost_by_async = False  # placeholer for future use
src_team_id = None
