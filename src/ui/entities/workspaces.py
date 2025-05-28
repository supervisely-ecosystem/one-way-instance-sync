import anyio
import os
import time
import asyncio
import shutil
from typing import List
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
from src.globals import boost_by_async
import requests
import subprocess
import src.globals as g
from PIL import Image

BATCH_SIZE = 50


class Scenario:
    CHECK = "check"
    REUPLOAD = "reupload"
    IGNORE = "ignore"
    NOT_SET = "not_set"


def change_link(bucket_path: str, link: str):
    parsed_url = urlparse(link)
    return f"{bucket_path}{parsed_url.path}"

def _transcode(path: str, output_path: str, video_codec: str = "libx264", audio_codec: str = "aac"):    
    pcs = subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-i",
            path,
            "-c:v",
            f"{video_codec}",
            "-c:a",
            f"{audio_codec}",
            "-vsync",
            "cfr",
            output_path,
        ],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if pcs.returncode != 0:
        raise RuntimeError(pcs.stderr)
    return output_path

def download_image_external_link(link: str, path: str):
    try:
        response = requests.get(link)
        with open(path, "wb") as fo:
            fo.write(response.content)
        with Image.open(path) as img:
            img.load()
    except Exception as e:
        sly.logger.warning(f"Failed to download image from external link: {link}.")
        raise e


def download_video_external_link(link: str, path: str):
    try:
        response = requests.get(link)
        with open(path, "wb") as fo:
            fo.write(response.content)

        result = subprocess.run(
            ["ffmpeg", "-v", "error", "-i", path, "-f", "null", "-"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if result.returncode != 0:
            raise ValueError(f"The video file '{path}' is corrupted.")
    except Exception as e:
        sly.logger.warning(f"Failed to download video from external link: {link}.")
        raise e


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
    global boost_by_async
    if boost_by_async:
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
            boost_by_async = False
    if not boost_by_async:
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
    already_downloaded_idx: List[int] = None,
):
    # for p in images_paths:
    #     silent_remove(p)

    dst_images = []
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
                dst_images.append(img)
            else:
                missing_images_ids.append(id)
                missing_images_paths.append(path)
                missing_images_names.append(name)
                missing_images_metas.append(meta)
        if missing_images_ids:
            if already_downloaded_idx is not None and len(already_downloaded_idx) > 0:
                filtered_ids = [
                    id for i, id in enumerate(missing_images_ids) if i not in already_downloaded_idx
                ]
                filtered_paths = [
                    path
                    for i, path in enumerate(missing_images_paths)
                    if i not in already_downloaded_idx
                ]
            else:
                filtered_ids = missing_images_ids
                filtered_paths = missing_images_paths
            if len(filtered_ids) > 0:
                for p in filtered_paths:
                    silent_remove(p)
                download_paths_async_or_sync(src_api, src_dataset.id, filtered_ids, filtered_paths)
            else:
                sly.logger.info("All images are already have been downloaded. Need just to upload.")
            imgs = dst_api.image.upload_paths(
                dataset_id=dst_dataset.id,
                names=missing_images_names,
                paths=missing_images_paths,
                metas=missing_images_metas,
            )
            dst_images.extend(imgs)
            for m_path in missing_images_paths:
                silent_remove(m_path)
        return dst_images
    if all([hash is not None for hash in images_hashs]):
        try:
            sly.logger.info("Attempting to upload images by hash.")
            valid_hashes = dst_api.image.check_existing_hashes(images_hashs)
            if len(valid_hashes) != len(images_hashs):
                raise Exception("Some hashes are not valid.")
            dst_images = dst_api.image.upload_hashes(
                dataset_id=dst_dataset.id,
                names=images_names,
                hashes=images_hashs,
                metas=images_metas,
            )
            return dst_images
        except Exception as e:
            sly.logger.info(
                f"Failed uploading images by hash. Attempting to upload images with paths."
            )
    if already_downloaded_idx is not None and len(already_downloaded_idx) > 0:
        filtered_ids = [id for i, id in enumerate(images_ids) if i not in already_downloaded_idx]
        filtered_paths = [
            path for i, path in enumerate(images_paths) if i not in already_downloaded_idx
        ]
    else:
        filtered_ids = images_ids
        filtered_paths = images_paths
    if len(filtered_ids) > 0:
        for p in filtered_paths:
            silent_remove(p)
        download_paths_async_or_sync(src_api, src_dataset.id, filtered_ids, filtered_paths)
    else:
        sly.logger.info("All images are already have been downloaded. Need just to upload.")
    dst_images = dst_api.image.upload_paths(
        dataset_id=dst_dataset.id,
        names=images_names,
        paths=images_paths,
        metas=images_metas,
    )
    for p in images_paths:
        silent_remove(p)
    return dst_images


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
    src_images: List[ImageInfo] = src_api.image.get_list(src_dataset.id)
    existing_images_list = dst_api.image.get_list(dst_dataset.id)
    existing_images = {}
    for img in existing_images_list:
        existing_images[img.name] = img

    with progress_items(
        message=f"Synchronizing images for Dataset: {src_dataset.name}", total=len(src_images)
    ) as pbar:
        pbar_correction = 0
        for images_batch in batched(src_images, BATCH_SIZE):
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
            images_links = [image.link for image in images_batch]
            download_with_paths_idx = [idx for idx, link in enumerate(images_links) if link is None]
            download_with_links_idx = [
                idx for idx, link in enumerate(images_links) if link is not None
            ]
            images_links = [link for link in images_links if link is not None]
            len_images_links = sum(1 for item in images_links if item is not None)

            if is_fast_mode and need_change_link:
                images_links = [change_link(bucket_path, link) for link in images_links]

            dst_uploaded_images = []
            if is_fast_mode:
                try:
                    if len_images_links > 0:
                        images_names_updated = [images_names[i] for i in download_with_links_idx]
                        images_metas_updated = [images_metas[i] for i in download_with_links_idx]
                        dst_uploaded_images.extend(
                            dst_api.image.upload_links(
                                dataset_id=dst_dataset.id,
                                names=images_names_updated,
                                links=images_links,
                                metas=images_metas_updated,
                                force_metadata_for_links=True,
                                skip_validation=False,
                            )
                        )

                        success = True
                        for image in dst_uploaded_images:
                            if image.width is None or image.height is None:
                                success = False
                                break
                        if success is False:
                            dst_api.image.remove_batch(
                                ids=[image.id for image in dst_uploaded_images]
                            )
                            sly.logger.warning(
                                "Links are not accessible or invalid. Attempting to download images with paths"
                            )
                            raise Exception(
                                "Links are not accessible or invalid. Attempting to download images with paths"
                            )
                    if len(download_with_paths_idx) > 0:
                        images_ids_updated = [images_ids[i] for i in download_with_paths_idx]
                        images_names_updated = [images_names[i] for i in download_with_paths_idx]
                        images_metas_updated = [images_metas[i] for i in download_with_paths_idx]
                        images_paths_updated = [images_paths[i] for i in download_with_paths_idx]
                        images_hashs_updated = [images_hashs[i] for i in download_with_paths_idx]

                        dst_uploaded_images.extend(
                            download_upload_images(
                                src_api,
                                dst_api,
                                src_dataset,
                                dst_dataset,
                                images_ids_updated,
                                images_paths_updated,
                                images_names_updated,
                                images_metas_updated,
                                images_hashs_updated,
                                existing_images,
                            )
                        )
                except Exception:
                    sly.logger.warning(
                        "Failed to upload images by links. Attempting to download images with paths."
                    )
                    successfully_downloaded = []
                    if len_images_links > 0:
                        for idx, link in zip(download_with_links_idx, images_links):
                            name = images_names[idx]
                            path = os.path.join(storage_dir, name)
                            if src_api.remote_storage.is_bucket_url(link):
                                src_api.storage.download(g.src_team_id, link, path)
                            else:
                                download_image_external_link(link, path)
                            successfully_downloaded.append(idx)

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
                        successfully_downloaded,
                    )
            else:
                successfully_downloaded = []
                if len_images_links > 0:
                    for idx, link in zip(download_with_links_idx, images_links):
                        name = images_names[idx]
                        path = os.path.join(storage_dir, name)
                        if src_api.remote_storage.is_bucket_url(link):
                            src_api.storage.download(g.src_team_id, link, path)
                        else:
                            download_image_external_link(link, path)
                        successfully_downloaded.append(idx)

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
                    successfully_downloaded,
                )

            dst_images_ids = [image.id for image in dst_uploaded_images]

            if len(dst_images_ids) > 0:
                annotations = src_api.annotation.download_json_batch(
                    dataset_id=src_dataset.id,
                    image_ids=images_ids,
                    force_metadata_for_links=False,
                )
                dst_api.annotation.upload_jsons(img_ids=dst_images_ids, ann_jsons=annotations)
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
    src_videos: List[VideoInfo] = src_api.video.get_list(
        dataset_id=src_dataset.id, raw_video_meta=True
    )
    if scenario == Scenario.CHECK:
        existing_videos_list = dst_api.video.get_list(dst_dataset.id)
        existing_videos = {
            existing_video.name: existing_video for existing_video in existing_videos_list
        }
    with progress_items(
        message=f"Synchronizing videos for Dataset: {src_dataset.name}", total=len(src_videos)
    ) as pbar:
        for src_video in src_videos:
            if scenario == Scenario.CHECK:
                if src_video.name in existing_videos:
                    dst_video = existing_videos[src_video.name]
                    if src_video.updated_at <= dst_video.updated_at:
                        pbar.update()
                        continue
                    else:
                        dst_api.video.remove(dst_video.id)
            try:
                if src_video.link is not None and is_fast_mode:
                    link = src_video.link
                    if need_change_link:
                        link = change_link(bucket_path, link)
                    dst_video = dst_api.video.upload_link(
                        dataset_id=dst_dataset.id,
                        link=link,
                        name=src_video.name,
                        skip_download=True,
                    )
                elif src_video.hash is not None:
                    dst_video = dst_api.video.upload_hash(
                        dataset_id=dst_dataset.id, name=src_video.name, hash=src_video.hash
                    )
                else:
                    raise ValueError(
                        f"No hash or link available for video '{src_video.name}'."
                        "Attempting to upload video with path."
                    )
            except Exception:
                video_path = os.path.join(storage_dir, src_video.name)
                download_path = True
                if src_video.link is not None:
                    try:
                        if src_api.remote_storage.is_bucket_url(src_video.link):
                            src_api.storage.download(g.src_team_id, src_video.link, video_path)
                        else:
                            download_video_external_link(src_video.link, video_path)
                        download_path = False
                    except Exception:
                        sly.logger.warning(
                            f"Failed to download video via link: {src_video.link}."
                            "Attempting to download video with path."
                        )
                        download_path = True
                if download_path:
                    src_api.video.download_path(id=src_video.id, path=video_path)
                if g.transcode_videos:
                    try:
                        output_path = _transcode(video_path, video_path + "_transcoded.mp4")
                    except Exception:
                        sly.logger.warning(
                            "Failed to transcode video: %s. It will be skipped.", video_path, exc_info=True
                        )
                    else:
                        result_path = video_path if video_path.endswith(".mp4") else video_path + ".mp4"
                        shutil.move(output_path, result_path)
                else:
                    result_path = video_path
                dst_video = dst_api.video.upload_path(
                    dataset_id=dst_dataset.id,
                    name=src_video.name,
                    path=result_path,
                    meta=src_video.meta,
                )
                silent_remove(video_path)
                silent_remove(result_path)

            ann_json = src_api.video.annotation.download(video_id=src_video.id)
            ann = sly.VideoAnnotation.from_json(
                data=ann_json, project_meta=meta, key_id_map=key_id_map
            )
            dst_api.video.annotation.append(video_id=dst_video.id, ann=ann, key_id_map=key_id_map)
            if src_video.custom_data is not None and len(src_video.custom_data) > 0:
                dst_api.video.update_custom_data(id=dst_video.id, data=src_video.custom_data)
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
    src_volumes: List[VolumeInfo] = src_api.volume.get_list(dataset_id=src_dataset.id)
    if scenario == Scenario.CHECK:
        existing_volumes_list = dst_api.volume.get_list(dst_dataset.id)
        existing_volumes = {
            existing_volume.name: existing_volume for existing_volume in existing_volumes_list
        }
    with progress_items(
        message=f"Synchronizing volumes for Dataset: {src_dataset.name}", total=len(src_volumes)
    ) as pbar:
        # sly.download_volume_project
        for src_volume in src_volumes:
            if scenario == Scenario.CHECK:
                if src_volume.name in existing_volumes:
                    dst_volume = existing_volumes[src_volume.name]
                    if src_volume.updated_at <= dst_volume.updated_at:
                        pbar.update()
                        continue
                    else:
                        dst_api.video.remove(dst_volume.id)  # method works for any entity type
            try:
                if src_volume.hash:
                    dst_volume = dst_api.volume.upload_hash(
                        dataset_id=dst_dataset.id,
                        name=src_volume.name,
                        hash=src_volume.hash,
                        meta=src_volume.meta,
                    )
                else:
                    raise ValueError(
                        f"No hash available for volume '{src_volume.name}'."
                        "Attempting to upload volume with path."
                    )
            except Exception:
                volume_path = os.path.join(storage_dir, src_volume.name)
                src_api.volume.download_path(id=src_volume.id, path=volume_path)
                dst_volume = dst_api.volume.upload_nrrd_serie_path(
                    dataset_id=dst_dataset.id, name=src_volume.name, path=volume_path
                )
                silent_remove(volume_path)

            ann_json = src_api.volume.annotation.download(volume_id=src_volume.id)
            ann = sly.VolumeAnnotation.from_json(
                data=ann_json, project_meta=meta, key_id_map=key_id_map
            )
            dst_api.volume.annotation.append(
                volume_id=dst_volume.id, ann=ann, key_id_map=key_id_map
            )
            if ann.spatial_figures:
                geometries = []
                for sf in ann_json.get("spatialFigures"):
                    sf_id = sf.get("id")
                    path = os.path.join(geometries_dir, f"{sf_id}.nrrd")
                    src_api.volume.figure.download_sf_geometries([sf_id], [path])
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
    src_pcds: List[PointcloudInfo] = src_api.pointcloud.get_list(dataset_id=src_dataset.id)
    if scenario == Scenario.CHECK:
        existing_pcds_list = dst_api.pointcloud.get_list(dst_dataset.id)
        existing_pcds = {existing_pcd.name: existing_pcd for existing_pcd in existing_pcds_list}
    with progress_items(
        message=f"Synchronizing point clouds for Dataset: {src_dataset.name}", total=len(src_pcds)
    ) as pbar:
        for src_pcd in src_pcds:
            if scenario == Scenario.CHECK:
                if src_pcd.name in existing_pcds:
                    dst_pcd = existing_pcds[src_pcd.name]
                    if src_pcd.updated_at <= dst_pcd.updated_at:
                        pbar.update()
                        continue
                    else:
                        dst_api.video.remove(dst_pcd.id)  # method works for any entity type
            try:
                if src_pcd.hash:
                    dst_pcd = dst_api.pointcloud.upload_hash(
                        dataset_id=dst_dataset.id,
                        name=src_pcd.name,
                        hash=src_pcd.hash,
                        meta=src_pcd.meta,
                    )
                else:
                    raise ValueError(
                        f"No hash available for point cloud '{src_pcd.name}'."
                        "Attempting to upload point cloud with path."
                    )
            except Exception:
                pcd_path = os.path.join(storage_dir, src_pcd.name)
                src_api.pointcloud.download_path(id=src_pcd.id, path=pcd_path)
                dst_pcd = dst_api.pointcloud.upload_path(
                    dataset_id=dst_dataset.id, name=src_pcd.name, path=pcd_path, meta=src_pcd.meta
                )
                silent_remove(pcd_path)

            ann_json = src_api.pointcloud.annotation.download(pointcloud_id=src_pcd.id)
            ann = sly.PointcloudAnnotation.from_json(
                data=ann_json, project_meta=meta, key_id_map=key_id_map_initial
            )
            dst_api.pointcloud.annotation.append(
                pointcloud_id=dst_pcd.id, ann=ann, key_id_map=key_id_map_new
            )
            rel_images = src_api.pointcloud.get_list_related_images(id=src_pcd.id)
            if len(rel_images) != 0:
                rimg_infos = []
                rimg_ids = []
                for rel_img in rel_images:
                    rimg_infos.append(
                        {
                            ApiField.ENTITY_ID: dst_pcd.id,
                            ApiField.NAME: rel_img[ApiField.NAME],
                            ApiField.HASH: rel_img[ApiField.HASH],
                            ApiField.META: rel_img[ApiField.META],
                        }
                    )
                    rimg_ids.append(rel_img[ApiField.ID])
                try:
                    dst_api.pointcloud.add_related_images(rimg_infos)
                except Exception:
                    sly.logger.info(
                        f"Failed to add related images to point cloud '{src_pcd.name}'."
                        "Attempting to upload related images with paths."
                    )
                    rimg_paths = []
                    for rimg_info, rimg_id in zip(rimg_infos, rimg_ids):
                        rimg_path = os.path.join(storage_dir, rimg_info[ApiField.NAME])
                        rimg_paths.append(rimg_path)
                        src_api.pointcloud.download_related_image(id=rimg_id, path=rimg_path)
                    dst_api.pointcloud.upload_related_images(rimg_paths)
                    dst_api.pointcloud.add_related_images(rimg_infos)
                    for rimg_path in rimg_paths:
                        silent_remove(rimg_path)

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
    src_pcdes = src_api.pointcloud_episode.get_list(dataset_id=src_dataset.id)
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
        total=len(src_pcdes),
    ) as pbar:
        for src_pcde in src_pcdes:
            if scenario == Scenario.CHECK:
                if src_pcde.name in existing_pcdes:
                    dst_pcde = existing_pcdes[src_pcde.name]
                    if src_pcde.updated_at <= dst_pcde.updated_at:
                        pbar.update()
                        continue
                    else:
                        dst_api.video.remove(dst_pcde.id)  # method works for any entity type
            try:
                if src_pcde.hash:
                    dst_pcde = dst_api.pointcloud_episode.upload_hash(
                        dataset_id=dst_dataset.id,
                        name=src_pcde.name,
                        hash=src_pcde.hash,
                        meta=src_pcde.meta,
                    )
                else:
                    raise ValueError(
                        f"No hash available for point cloud episode '{src_pcde.name}'."
                        "Attempting to upload point cloud episode with path."
                    )
            except Exception:
                pcde_path = os.path.join(storage_dir, src_pcde.name)
                src_api.pointcloud_episode.download_path(id=src_pcde.id, path=pcde_path)
                dst_pcde = dst_api.pointcloud_episode.upload_path(
                    dataset_id=dst_dataset.id,
                    name=src_pcde.name,
                    path=pcde_path,
                    meta=src_pcde.meta,
                )
                silent_remove(pcde_path)

            frame_to_pointcloud_ids[dst_pcde.meta["frame"]] = dst_pcde.id
            rel_images = src_api.pointcloud_episode.get_list_related_images(id=src_pcde.id)
            if len(rel_images) != 0:
                rimg_infos = []
                rimg_ids = []
                for rel_img in rel_images:
                    rimg_infos.append(
                        {
                            ApiField.ENTITY_ID: dst_pcde.id,
                            ApiField.NAME: rel_img[ApiField.NAME],
                            ApiField.HASH: rel_img[ApiField.HASH],
                            ApiField.META: rel_img[ApiField.META],
                        }
                    )
                    rimg_ids.append(rel_img[ApiField.ID])
                try:
                    dst_api.pointcloud_episode.add_related_images(rimg_infos)
                except Exception:
                    sly.logger.info(
                        f"Failed to add related images to point cloud episode'{src_pcde.name}'."
                        "Attempting to upload related images with paths."
                    )
                    rimg_paths = []
                    for rimg_info, rimg_id in zip(rimg_infos, rimg_ids):
                        rimg_path = os.path.join(storage_dir, rimg_info[ApiField.NAME])
                        rimg_paths.append(rimg_path)
                        src_api.pointcloud.download_related_image(id=rimg_id, path=rimg_path)
                    dst_api.pointcloud.upload_related_images(rimg_paths)
                    dst_api.pointcloud.add_related_images(rimg_infos)
                    for rimg_path in rimg_paths:
                        silent_remove(rimg_path)
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
    src_team = src_api.team.get_info_by_id(team_id)
    g.src_team_id = team_id

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

    dst_team = dst_api.team.get_info_by_name(src_team.name)
    if dst_team is None:
        dst_team = dst_api.team.create(src_team.name, description=src_team.description)

    with progress_ws(
        message=f"Synchronizing workspaces for Team: {src_team.name}", total=len(workspaces)
    ) as pbar_ws:
        for workspace in workspaces:
            dst_workspace = dst_api.workspace.get_info_by_name(dst_team.id, workspace.name)
            if dst_workspace is None:
                dst_workspace = dst_api.workspace.create(
                    dst_team.id, workspace.name, description=workspace.description
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
                    dst_project = dst_api.project.get_info_by_name(dst_workspace.id, project.name)
                    # if (
                    #     dst_project is not None
                    #     and dst_project.type != str(sly.ProjectType.IMAGES)
                    #     and temp_ws_scenario == Scenario.CHECK
                    # ):
                    #     temp_ws_scenario = Scenario.REUPLOAD
                    #     sly.logger.info(
                    #         f"Changing synchronization scenario to 'reupload' for non-image projects '{dst_project.name}'."
                    #     )

                    if dst_project is None:
                        dst_project = dst_api.project.create(
                            dst_workspace.id,
                            project.name,
                            description=project.description,
                            type=project.type,
                        )
                    elif dst_project is not None and temp_ws_scenario == Scenario.REUPLOAD:
                        dst_api.project.remove(dst_project.id)
                        dst_project = dst_api.project.create(
                            dst_workspace.id,
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
