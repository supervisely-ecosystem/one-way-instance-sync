import anyio
import os
import time
import asyncio
from typing import List, Dict
import supervisely as sly
from urllib.parse import urlparse
from supervisely import batched, KeyIdMap, DatasetInfo
from supervisely.project.project_type import ProjectType
from supervisely.app.widgets import Progress
from supervisely.api.module_api import ApiField
from supervisely.api.image_api import ImageInfo
from supervisely.api.video.video_api import VideoInfo
from supervisely.api.volume.volume_api import VolumeInfo
from supervisely.api.pointcloud.pointcloud_api import PointcloudInfo
from supervisely.io.fs import mkdir, silent_remove

BATCH_SIZE = 50


class Scenario:
    CHECK = "check"
    REUPLOAD = "reupload"
    IGNORE = "ignore"
    NOT_SET = "not_set"


def change_link(bucket_path: str, link: str):
    parsed_url = urlparse(link)
    return f"{bucket_path}{parsed_url.path}"


def retry_if_end_stream(func):
    # decorator to retry 5 times function if it raises EndOfStream exception
    def wrapper(*args, **kwargs):
        for i in range(5):
            try:
                return func(*args, **kwargs)
            except (anyio.EndOfStream, FileNotFoundError) as e:
                if i == 4:
                    raise e
                sly.logger.warning(
                    f"Error occurred while downloading/uploading images. Retrying... {i + 1}/5"
                )
                time.sleep(2)

    return wrapper


def download_paths_async_or_sync(api: sly.Api, dataset_id: int, ids: List[int], paths: List[str]):
    try:
        download_coro = api.image.download_paths_async(ids, paths)
        loop = sly.utils.get_or_create_event_loop()
        if loop.is_running():
            future = asyncio.run_coroutine_threadsafe(download_coro, loop=loop)
            future.result()
        else:
            loop.run_until_complete(download_coro)
    except Exception as e:
        sly.logger.warning(
            "Failed to download images asynchronously. Downloading images synchronously."
        )
        api.image.download_paths(dataset_id, ids, paths)


@retry_if_end_stream
def download_upload_images(
    src_api: sly.Api,
    dst_api: sly.Api,
    src_dataset: DatasetInfo,
    dst_dataset: DatasetInfo,
    images_ids: List[int],
    images_paths: List[str],
    images_names: List[str],
    images_metas: List[dict],
    images_hashs: List[str],
    existing_images,
):
    for p in images_paths:
        silent_remove(p)

    res_images = []
    if all([name in existing_images for name in images_names]):
        sly.logger.info("Current batch of images already exist in destination dataset. Skipping...")
        return [existing_images[name] for name in images_names]
    elif any([name in existing_images for name in images_names]):
        sly.logger.info(
            "Some images in batch already exist in destination dataset. Downloading only missing images."
        )
        missing_images_ids = []
        missing_images_paths = []
        missing_images_names = []
        missing_images_metas = []
        for id, path, name, meta in zip(images_ids, images_paths, images_names, images_metas):
            if name in existing_images:
                img = existing_images[name]
                res_images.append(img)
            else:
                missing_images_ids.append(id)
                missing_images_paths.append(path)
                missing_images_names.append(name)
                missing_images_metas.append(meta)
                # src_api.image.download_path(id, path)
                # img = dst_api.image.upload_path(
                #     dataset_id=dst_dataset.id,
                #     name=name,
                #     path=path,
                #     meta=meta,
                # )
                # silent_remove(path)
                # res_images.append(img)
        if missing_images_ids:
            download_paths_async_or_sync(
                src_api, src_dataset.id, missing_images_ids, missing_images_paths
            )
            # src_api.image.download_paths(missing_images_ids, missing_images_paths)
            imgs = dst_api.image.upload_paths(
                dataset_id=dst_dataset.id,
                names=missing_images_names,
                paths=missing_images_paths,
                metas=missing_images_metas,
            )
            res_images.extend(imgs)
            for m_path in missing_images_paths:
                silent_remove(m_path)
        return res_images
    if all([hash is not None for hash in images_hashs]):
        try:
            sly.logger.info("Attempting to upload images by hash.")
            valid_hashes = dst_api.image.check_existing_hashes(images_hashs)
            if len(valid_hashes) != len(images_hashs):
                raise Exception("Some hashes are not valid.")
            res_images = dst_api.image.upload_hashes(
                dataset_id=dst_dataset.id,
                names=images_names,
                hashes=images_hashs,
                metas=images_metas,
            )
            return res_images
        except Exception as e:
            sly.logger.info(
                f"Failed uploading images by hash. Attempting to upload images with paths."
            )
    download_paths_async_or_sync(src_api, src_dataset.id, images_ids, images_paths)
    # src_api.image.download_paths(
    #     dataset_id=src_dataset.id,
    #     ids=images_ids,
    #     paths=images_paths,
    # )
    res_images = dst_api.image.upload_paths(
        dataset_id=dst_dataset.id,
        names=images_names,
        paths=images_paths,
        metas=images_metas,
    )
    for p in images_paths:
        silent_remove(p)
    return res_images


def process_images(
    dst_api: sly.Api,
    src_api: sly.Api,
    src_dataset: sly.DatasetInfo,
    dst_dataset: sly.DatasetInfo,
    meta: sly.ProjectMeta,
    progress_items: Progress,
    is_fast_mode: bool = False,
    need_change_link: bool = False,
    bucket_path: str = None,
    scenario: str = Scenario.NOT_SET,
):
    storage_dir = "storage"
    mkdir(storage_dir, True)
    images: List[ImageInfo] = src_api.image.get_list(src_dataset.id)
    existing_images_list = dst_api.image.get_list(dst_dataset.id)
    existing_images = {}
    for img in existing_images_list:
        existing_images[img.name] = img

    with progress_items(
        message=f"Synchronizing images for Dataset: {src_dataset.name}", total=len(images)
    ) as pbar:
        pbar_correction = 0
        for images_batch in batched(images, BATCH_SIZE):
            if scenario == Scenario.CHECK:
                images_batch_download = []
                for image in images_batch:
                    if image.name not in existing_images:
                        images_batch_download.append(image)
                    else:
                        if image.updated_at > existing_images[image.name].updated_at:
                            images_batch_download.append(image)
                pbar_correction = len(images_batch) - len(images_batch_download)
                images_batch = images_batch_download
            images_ids = [image.id for image in images_batch]
            images_names = [image.name for image in images_batch]
            images_metas = [image.meta for image in images_batch]
            images_paths = [os.path.join(storage_dir, image_name) for image_name in images_names]
            images_hashs = [image.hash for image in images_batch]

            images_links = []
            if is_fast_mode:
                for image in images:
                    if image.link is not None:
                        link = image.link
                        if need_change_link:
                            link = change_link(bucket_path, link)
                        images_links.append(link)

            if len(images_links) == len(images_batch):
                try:
                    dst_uploaded_images = dst_api.image.upload_links(
                        dataset_id=dst_dataset.id,
                        names=images_names,
                        links=images_links,
                        metas=images_metas,
                        force_metadata_for_links=False,
                        skip_validation=False,
                    )

                    success = True
                    for image in dst_uploaded_images:
                        if image.width is None or image.height is None:
                            success = False
                            break
                    if success is False:
                        dst_api.image.remove_batch(ids=[image.id for image in dst_uploaded_images])
                        sly.logger.warning(
                            "Links are not accessible or invalid. Attempting to download images with paths"
                        )
                        raise Exception(
                            "Links are not accessible or invalid. Attempting to download images with paths"
                        )

                except Exception:
                    dst_uploaded_images = download_upload_images(
                        src_api,
                        dst_api,
                        src_dataset,
                        dst_dataset,
                        images_ids,
                        images_paths,
                        images_names,
                        images_metas,
                        images_hashs,
                        existing_images,
                    )
            else:
                dst_uploaded_images = download_upload_images(
                    src_api,
                    dst_api,
                    src_dataset,
                    dst_dataset,
                    images_ids,
                    images_paths,
                    images_names,
                    images_metas,
                    images_hashs,
                    existing_images,
                )

            res_images_ids = [image.id for image in dst_uploaded_images]

            # check if need to update annotations for existing images
            # if scenario == Scenario.CHECK and len(existing_images_list) != 0:
            #     existing_to_update = []
            #     for name, updated_at in zip(images_names, images_updated_at):
            #         existing_image: ImageInfo = existing_images.get(name, None)
            #         if existing_image is not None and existing_image.updated_at < updated_at:
            #             existing_to_update.append(existing_image.id)
            #     if len(existing_to_update) > 0:
            #         res_images_ids.extend(existing_to_update)
            #         res_images_ids = list(set(res_images_ids))

            annotations = src_api.annotation.download_json_batch(
                dataset_id=src_dataset.id,
                image_ids=images_ids,
                force_metadata_for_links=False,
            )
            dst_api.annotation.upload_jsons(img_ids=res_images_ids, ann_jsons=annotations)
            pbar.update(len(images_batch) + pbar_correction)


def process_videos(
    dst_api: sly.Api,
    src_api: sly.Api,
    src_dataset: DatasetInfo,
    dst_dataset: DatasetInfo,
    meta: sly.ProjectMeta,
    progress_items: Progress,
    is_fast_mode: bool = False,
    need_change_link: bool = False,
    bucket_path: str = None,
    scenario: str = Scenario.NOT_SET,
):
    storage_dir = "storage"
    mkdir(storage_dir, True)
    key_id_map = KeyIdMap()
    videos: List[VideoInfo] = src_api.video.get_list(dataset_id=src_dataset.id, raw_video_meta=True)
    if scenario == Scenario.CHECK:
        existing_videos_list = dst_api.video.get_list(dst_dataset.id)
        existing_videos = {
            existing_video.name: existing_video for existing_video in existing_videos_list
        }
    with progress_items(
        message=f"Synchronizing videos for Dataset: {src_dataset.name}", total=len(videos)
    ) as pbar:
        for video in videos:
            if scenario == Scenario.CHECK:
                if video.name in existing_videos:
                    if video.updated_at <= existing_videos[video.name].updated_at:
                        pbar.update()
                    continue
            try:
                if video.link is not None and is_fast_mode:
                    link = video.link
                    if need_change_link:
                        link = change_link(bucket_path, link)
                    res_video = dst_api.video.upload_link(
                        dataset_id=dst_dataset.id, link=link, name=video.name, skip_download=True
                    )
                elif video.hash is not None:
                    res_video = dst_api.video.upload_hash(
                        dataset_id=dst_dataset.id, name=video.name, hash=video.hash
                    )
            except Exception:
                video_path = os.path.join(storage_dir, video.name)
                src_api.video.download_path(id=video.id, path=video_path)
                res_video = dst_api.video.upload_path(
                    dataset_id=dst_dataset.id,
                    name=video.name,
                    path=video_path,
                    meta=video.meta,
                )
                silent_remove(video_path)

            ann_json = src_api.video.annotation.download(video_id=video.id)
            ann = sly.VideoAnnotation.from_json(
                data=ann_json, project_meta=meta, key_id_map=key_id_map
            )
            dst_api.video.annotation.append(video_id=res_video.id, ann=ann, key_id_map=key_id_map)
            pbar.update()


def process_volumes(
    dst_api: sly.Api,
    src_api: sly.Api,
    src_dataset: DatasetInfo,
    dst_dataset: DatasetInfo,
    meta: sly.ProjectMeta,
    progress_items: Progress,
    is_fast_mode: bool = False,
    need_change_link: bool = False,
    bucket_path: str = None,
    scenario: str = Scenario.NOT_SET,
):
    storage_dir = "storage"
    mkdir(storage_dir, True)
    key_id_map = KeyIdMap()
    geometries_dir = f"geometries_{src_dataset.id}"
    sly.fs.mkdir(geometries_dir, True)
    volumes: List[VolumeInfo] = src_api.volume.get_list(dataset_id=src_dataset.id)
    if scenario == Scenario.CHECK:
        existing_volumes_list = dst_api.volume.get_list(dst_dataset.id)
        existing_volumes = {
            existing_volume.name: existing_volume for existing_volume in existing_volumes_list
        }
    with progress_items(
        message=f"Synchronizing volumes for Dataset: {src_dataset.name}", total=len(volumes)
    ) as pbar:
        # sly.download_volume_project
        for volume in volumes:
            if scenario == Scenario.CHECK:
                if volume.name in existing_volumes:
                    if volume.updated_at <= existing_volumes[volume.name].updated_at:
                        pbar.update()
                    continue
            if volume.hash:
                res_volume = dst_api.volume.upload_hash(
                    dataset_id=dst_dataset.id,
                    name=volume.name,
                    hash=volume.hash,
                    meta=volume.meta,
                )
            else:
                volume_path = os.path.join(storage_dir, volume.name)
                src_api.volume.download_path(id=volume.id, path=volume_path)
                res_volume = dst_api.volume.upload_nrrd_serie_path(
                    dataset_id=dst_dataset.id, name=volume.name, path=volume_path
                )
                silent_remove(volume_path)

            ann_json = src_api.volume.annotation.download(volume_id=volume.id)
            ann = sly.VolumeAnnotation.from_json(
                data=ann_json, project_meta=meta, key_id_map=key_id_map
            )
            dst_api.volume.annotation.append(
                volume_id=res_volume.id, ann=ann, key_id_map=key_id_map
            )
            if ann.spatial_figures:
                geometries = []
                for sf in ann_json.get("spatialFigures"):
                    sf_id = sf.get("id")
                    path = os.path.join(geometries_dir, f"{sf_id}.nrrd")
                    src_api.volume.figure.download_stl_meshes([sf_id], [path])
                    with open(path, "rb") as file:
                        geometry_bytes = file.read()
                    geometries.append(geometry_bytes)
                dst_api.volume.figure.upload_sf_geometry(
                    ann.spatial_figures, geometries, key_id_map=key_id_map
                )
                del geometries
            pbar.update()
        sly.fs.remove_dir(geometries_dir)


def process_pcd(
    dst_api: sly.Api,
    src_api: sly.Api,
    src_dataset: DatasetInfo,
    dst_dataset: DatasetInfo,
    meta: sly.ProjectMeta,
    progress_items: Progress,
    is_fast_mode: bool = False,
    need_change_link: bool = False,
    bucket_path: str = None,
    scenario: str = Scenario.NOT_SET,
):
    storage_dir = "storage"
    mkdir(storage_dir, True)
    key_id_map_initial = KeyIdMap()
    key_id_map_new = KeyIdMap()
    pcds: List[PointcloudInfo] = src_api.pointcloud.get_list(dataset_id=src_dataset.id)
    if scenario == Scenario.CHECK:
        existing_pcds_list = dst_api.pointcloud.get_list(dst_dataset.id)
        existing_pcds = {existing_pcd.name: existing_pcd for existing_pcd in existing_pcds_list}
    with progress_items(
        message=f"Synchronizing point clouds for Dataset: {src_dataset.name}", total=len(pcds)
    ) as pbar:
        for pcd in pcds:
            if scenario == Scenario.CHECK:
                if pcd.name in existing_pcds:
                    if pcd.updated_at <= existing_pcds[pcd.name].updated_at:
                        pbar.update()
                    continue
            if pcd.hash:
                res_pcd = dst_api.pointcloud.upload_hash(
                    dataset_id=dst_dataset.id,
                    name=pcd.name,
                    hash=pcd.hash,
                    meta=pcd.meta,
                )
            else:
                pcd_path = os.path.join(storage_dir, pcd.name)
                src_api.pointcloud.download_path(id=pcd.id, path=pcd_path)
                res_pcd = dst_api.pointcloud.upload_path(
                    dataset_id=dst_dataset.id, name=pcd.name, path=pcd_path, meta=pcd.meta
                )
                silent_remove(pcd_path)

            ann_json = src_api.pointcloud.annotation.download(pointcloud_id=pcd.id)
            ann = sly.PointcloudAnnotation.from_json(
                data=ann_json, project_meta=meta, key_id_map=key_id_map_initial
            )
            dst_api.pointcloud.annotation.append(
                pointcloud_id=res_pcd.id, ann=ann, key_id_map=key_id_map_new
            )
            rel_images = src_api.pointcloud.get_list_related_images(id=pcd.id)
            if len(rel_images) != 0:
                rimg_infos = []
                for rel_img in rel_images:
                    rimg_infos.append(
                        {
                            ApiField.ENTITY_ID: res_pcd.id,
                            ApiField.NAME: rel_img[ApiField.NAME],
                            ApiField.HASH: rel_img[ApiField.HASH],
                            ApiField.META: rel_img[ApiField.META],
                        }
                    )
                dst_api.pointcloud.add_related_images(rimg_infos)

            pbar.update()


def process_pcde(
    dst_api: sly.Api,
    src_api: sly.Api,
    src_dataset: DatasetInfo,
    dst_dataset: DatasetInfo,
    meta: sly.ProjectMeta,
    progress_items: Progress,
    is_fast_mode: bool = False,
    need_change_link: bool = False,
    bucket_path: str = None,
    scenario: str = Scenario.NOT_SET,
):
    storage_dir = "storage"
    mkdir(storage_dir, True)
    key_id_map = KeyIdMap()
    pcdes = src_api.pointcloud_episode.get_list(dataset_id=src_dataset.id)
    ann_json = src_api.pointcloud_episode.annotation.download(dataset_id=src_dataset.id)
    ann = sly.PointcloudEpisodeAnnotation.from_json(
        data=ann_json, project_meta=meta, key_id_map=KeyIdMap()
    )
    if scenario == Scenario.CHECK:
        existing_pcdes_list = dst_api.pointcloud_episode.get_list(dst_dataset.id)
        existing_pcdes = {
            existing_pcde.name: existing_pcde for existing_pcde in existing_pcdes_list
        }
    frame_to_pointcloud_ids = {}
    with progress_items(
        message=f"Synchronizing point cloud episodes for Dataset: {src_dataset.name}",
        total=len(pcdes),
    ) as pbar:
        for pcde in pcdes:
            if scenario == Scenario.CHECK:
                if pcde.name in existing_pcdes:
                    if pcde.updated_at <= existing_pcdes[pcde.name].updated_at:
                        pbar.update()
                    continue
            if pcde.hash:
                res_pcde = dst_api.pointcloud_episode.upload_hash(
                    dataset_id=dst_dataset.id,
                    name=pcde.name,
                    hash=pcde.hash,
                    meta=pcde.meta,
                )
            else:
                pcde_path = os.path.join(storage_dir, pcde.name)
                src_api.pointcloud_episode.download_path(id=pcde.id, path=pcde_path)
                res_pcde = dst_api.pointcloud_episode.upload_path(
                    dataset_id=dst_dataset.id, name=pcde.name, path=pcde_path, meta=pcde.meta
                )
                silent_remove(pcde_path)

            frame_to_pointcloud_ids[res_pcde.meta["frame"]] = res_pcde.id
            rel_images = src_api.pointcloud_episode.get_list_related_images(id=pcde.id)
            if len(rel_images) != 0:
                rimg_infos = []
                for rel_img in rel_images:
                    rimg_infos.append(
                        {
                            ApiField.ENTITY_ID: res_pcde.id,
                            ApiField.NAME: rel_img[ApiField.NAME],
                            ApiField.HASH: rel_img[ApiField.HASH],
                            ApiField.META: rel_img[ApiField.META],
                        }
                    )
                dst_api.pointcloud_episode.add_related_images(rimg_infos)
            pbar.update()

        dst_api.pointcloud_episode.annotation.append(
            dataset_id=dst_dataset.id,
            ann=ann,
            frame_to_pointcloud_ids=frame_to_pointcloud_ids,
            key_id_map=key_id_map,
        )


def get_ws_projects_map(ws_collapse):
    ws_projects_map = {}
    for ws in ws_collapse._items:
        ws_projects_map[ws.name] = []
        projects = ws.content
        for project in projects.get_transferred_items():
            ws_projects_map[ws.name].append(project)
    return ws_projects_map


process_type_map = {
    ProjectType.IMAGES.value: process_images,
    ProjectType.VIDEOS.value: process_videos,
    ProjectType.VOLUMES.value: process_volumes,
    ProjectType.POINT_CLOUDS.value: process_pcd,
    ProjectType.POINT_CLOUD_EPISODES.value: process_pcde,
}


def import_workspaces(
    dst_api: sly.Api,
    src_api: sly.Api,
    team_id: int,
    ws_collapse: sly.app.widgets.Collapse,
    progress_ws: Progress,
    progress_pr: Progress,
    progress_ds: Progress,
    progress_items: Progress,
    is_import_all_ws: bool = False,
    ws_scenario_value: str = Scenario.CHECK,
    is_fast_mode: bool = False,
    change_link_flag: bool = False,
    bucket_path: str = None,
):
    team = src_api.team.get_info_by_id(team_id)

    if is_import_all_ws:
        workspaces = src_api.workspace.get_list(team_id=team_id)
    else:
        ws_projects_map = get_ws_projects_map(ws_collapse)
        for ws in ws_collapse._items:
            ws_projects_map[ws.name] = []
            projects = ws.content
            for project in projects.get_transferred_items():
                ws_projects_map[ws.name].append(project)
        workspaces = [
            src_api.workspace.get_info_by_id(workspace_id)
            for workspace_id in ws_projects_map
            if len(ws_projects_map[workspace_id]) > 0
        ]

    res_team = dst_api.team.get_info_by_name(team.name)
    if res_team is None:
        res_team = dst_api.team.create(team.name, description=team.description)

    with progress_ws(
        message=f"Synchronizing workspaces for Team: {team.name}", total=len(workspaces)
    ) as pbar_ws:
        for workspace in workspaces:
            res_workspace = dst_api.workspace.get_info_by_name(res_team.id, workspace.name)
            if res_workspace is None:
                res_workspace = dst_api.workspace.create(
                    res_team.id, workspace.name, description=workspace.description
                )

            if is_import_all_ws:
                projects = src_api.project.get_list(workspace.id)
            else:
                projects = [
                    src_api.project.get_info_by_id(project_id)
                    for project_id in ws_projects_map[workspace.id]
                ]
            with progress_pr(
                message=f"Synchronizing projects for Workspace: {workspace.name}",
                total=len(projects),
            ) as pbar_pr:
                for project in projects:
                    temp_ws_scenario = ws_scenario_value
                    dst_project = dst_api.project.get_info_by_name(res_workspace.id, project.name)
                    # if (
                    #     dst_project is not None
                    #     and dst_project.type != str(sly.ProjectType.IMAGES)
                    #     and temp_ws_scenario == Scenario.CHECK
                    # ):
                    #     temp_ws_scenario = Scenario.IGNORE
                    #     sly.logger.info(
                    #         "Changing synchronization scenario to 'ignore' for non-image projects."
                    #     )
                    if dst_project is None:
                        dst_project = dst_api.project.create(
                            res_workspace.id,
                            project.name,
                            description=project.description,
                            type=project.type,
                        )
                    elif dst_project is not None and temp_ws_scenario == Scenario.REUPLOAD:
                        dst_api.project.remove(dst_project.id)
                        dst_project = dst_api.project.create(
                            res_workspace.id,
                            project.name,
                            description=project.description,
                            type=project.type,
                        )

                    elif dst_project is not None and temp_ws_scenario == Scenario.IGNORE:
                        sly.logger.info(
                            f"Project {project.name} already exists in destination Workspace. Skipping..."
                        )
                        pbar_pr.update()
                        continue

                    elif dst_project is not None and temp_ws_scenario == Scenario.CHECK:
                        sly.logger.info(
                            f"Project {project.name} already exists in destination Workspace. Checking..."
                        )

                    meta_json = src_api.project.get_meta(project.id)
                    dst_api.project.update_meta(dst_project.id, meta_json)
                    meta = sly.ProjectMeta.from_json(meta_json)

                    ds_mapping = {}
                    datasets = src_api.dataset.get_list(project.id, recursive=True)
                    with progress_ds(
                        message=f"Synchronizing datasets for Project: {project.name}",
                        total=len(datasets),
                    ) as pbar_ds:
                        for src_dataset in datasets:
                            dst_parent = None
                            if src_dataset.parent_id is not None:
                                try:
                                    dst_parent = ds_mapping[src_dataset.parent_id]
                                except KeyError:
                                    sly.logger.warning(
                                        f"Parent dataset {src_dataset.parent_id} not found in mapping for dataset '{src_dataset.name}'."
                                        "Creating dataset at the top level of project."
                                    )
                            dst_dataset = dst_api.dataset.get_info_by_name(
                                dst_project.id,
                                src_dataset.name,
                                parent_id=dst_parent,
                            )

                            if dst_dataset is None:
                                dst_dataset = dst_api.dataset.create(
                                    dst_project.id,
                                    src_dataset.name,
                                    description=src_dataset.description,
                                    parent_id=dst_parent,
                                )

                            ds_mapping[src_dataset.id] = dst_dataset.id

                            process_func = process_type_map.get(project.type)
                            process_func(
                                dst_api=dst_api,
                                src_api=src_api,
                                src_dataset=src_dataset,
                                dst_dataset=dst_dataset,
                                meta=meta,
                                progress_items=progress_items,
                                is_fast_mode=is_fast_mode,
                                need_change_link=change_link_flag,
                                bucket_path=bucket_path,
                                scenario=temp_ws_scenario,
                            )
                            pbar_ds.update()
                    pbar_pr.update()
            pbar_ws.update()

    # progress_ws.hide()
    # progress_pr.hide()
    # progress_ds.hide()
    # progress_items.hide()