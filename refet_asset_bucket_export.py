import argparse
import json
import logging
import time

import ee
from google.cloud import storage

import openet.core.utils as utils

logging.getLogger('earthengine-api').setLevel(logging.INFO)
logging.getLogger('googleapiclient').setLevel(logging.INFO)
logging.getLogger('requests').setLevel(logging.INFO)
logging.getLogger('urllib3').setLevel(logging.INFO)

TIMESTEPS = ['daily', 'monthly']
REGIONS = ['conus/gridmet', 'california/cimis']


def main(
        timestep,
        region,
        start_dt,
        end_dt,
        bucket_name,
        project_id,
        delay_time=0,
        ready_task_max=-1,
        export_properties_json=True,
        reverse_flag=False,
        gee_key_file=None,
):
    """Export OpenET referencet ET assets to bucket

    Parameters
    ----------
    timestep : {'monthly', 'daily'}
        Data timestep.
    region : str

    bucket_name : str
        Google Cloud Storage bucket name.
    project_id : str
        Google Cloud project ID.
    start_dt : datetime
        Start date.
    end_dt : datetime
        End date (exclusive).
    delay_time : float, optional
        Delay time in seconds between starting export tasks (or checking the
        number of queued tasks, see "ready_task_max" parameter).
        The default is 0.
    ready_task_max : int, optional
        Maximum number of queued "READY" tasks.  The default is -1 which is
        implies no limit to the number of tasks that will be submitted.
    export_properties_json : bool, optional
        Export a properties JSON file for each image.
    reverse_flag : bool, optional
        If True, process WRS2 tiles in reverse order (the default is False).
    gee_key_file : str, None, optional
        Earth Engine service account JSON key file (the default is None).
        If set, this will be used instead of the cloud project ID for
        initializing/authenticating GEE.

    """
    logging.info(f'\nExport reference ET {region} {timestep} assets to bucket')

    start_date = start_dt.strftime('%Y-%m-%d')
    end_date = end_dt.strftime('%Y-%m-%d')
    logging.info(f'  Start: {start_date}')
    logging.info(f'  End:   {end_date}')

    # Set the export parameters based on the region
    # Hardcoding for now but these could be read dynamically from the input collection
    if region.lower() == 'conus/gridmet':
        version = 'v1'
        input_coll_id = f'projects/openet/assets/reference_et/{region.lower()}/{timestep.lower()}/{version}'
        bucket_folder = f'reference_et/{region.lower()}/{timestep.lower()}/{version}'
        export_params = {
            'crs': 'EPSG:4326',
            'dimensions': '1386x585',
            'crsTransform': [0.041666666666666664, 0, -124.7875, 0, -0.041666666666666664, 49.42083333333334],
            'maxPixels': int(1E10),
            'fileFormat': 'GeoTIFF',
            'formatOptions': {'cloudOptimized': False, 'noData': -9999},
            'fileFormat': 'GeoTIFF',
            # 'fileDimensions': 65536,
        }
    elif region.lower() == 'california/cimis':
        version = 'v1'
        input_coll_id = f'projects/openet/assets/reference_et/{region.lower()}/{timestep.lower()}/{version}'
        bucket_folder = f'reference_et/{region.lower()}/{timestep.lower()}/{version}'
        export_params = {
            'crs': 'EPSG:3310',
            'dimensions': '510x560',
            'crsTransform': [2000, 0, -410000.0, 0, -2000, 460000.0],
            'maxPixels': int(1E10),
            'fileFormat': 'GeoTIFF',
            'formatOptions': {'cloudOptimized': False, 'noData': -9999},
        }
    else:
        raise ValueError(f'Unsupported region: {region}')

    logging.debug(f'  Shape:      {export_params["dimensions"]}')
    logging.debug(f'  Transform:  {export_params["crsTransform"]}')
    # logging.debug(f'  Extent:     {export_params["extent"]}')
    # logging.debug(f'  MaxPixels:  {export_params["maxpixels"]}')

    if timestep.lower() == 'monthly':
        asset_dt_fmt = '%Y%m'
    elif timestep.lower() == 'daily':
        asset_dt_fmt = '%Y%m%d'
    else:
        raise ValueError(f'Unsupported timestep: {timestep}')

    # Initialize Earth Engine
    if gee_key_file:
        logging.info(f'\nInitializing GEE using user key file: {gee_key_file}')
        try:
            ee.Initialize(ee.ServiceAccountCredentials('_', key_file=gee_key_file))
        except ee.ee_exception.EEException:
            logging.warning('Unable to initialize GEE using user key file')
            return False
    elif project_id is not None:
        logging.info(f'\nInitializing Earth Engine using project credentials'
                     f'\n  Project ID: {project_id}')
        try:
            ee.Initialize(project=project_id)
        except Exception as e:
            logging.warning(f'\nUnable to initialize GEE using project ID\n  {e}')
            return False
    else:
        logging.info('\nInitializing Earth Engine using user credentials')
        ee.Initialize()

    ee.data.setWorkloadTag(
        f'reference_et-{region.lower().replace("/", "_")}-{timestep.lower()}-bucket-export'
    )

    logging.info('\nChecking task list')
    tasks = utils.get_ee_tasks()
    ready_task_count = sum(1 for t in tasks.values() if t['state'] == 'READY')
    # Hold the job here if the ready task count is already over the max
    ready_task_count = utils.delay_task(
        delay_time=0, task_max=ready_task_max, task_count=ready_task_count
    )

    logging.info(f'\nReading bucket files')
    storage_client = storage.Client(project=project_id)
    bucket_object = storage_client.get_bucket(bucket_name)
    bucket_files = {x.name for x in bucket_object.list_blobs(prefix=bucket_folder)}


    logging.info('\nGetting list of available input assets')
    logging.info(f'  {input_coll_id}')
    input_coll = ee.ImageCollection(input_coll_id).filterDate(start_dt, end_dt)
    input_asset_props = {
        f'{ftr["properties"]["system:index"]}': ftr
        for ftr in utils.get_info(input_coll)['features']
    }
    input_id_list = list(input_asset_props.keys())
    if not input_id_list:
        logging.info('  No source images in date range')


    logging.info('\nProcessing image asset list')
    for image_id in sorted(input_id_list, reverse=reverse_flag):
        logging.info(f'{image_id}')

        input_img_id = f'{input_coll_id}/{image_id}'
        bucket_img = f'{bucket_folder}/{image_id}.tif'
        bucket_json = f'{bucket_folder}/{image_id}_properties.json'
        logging.debug(f'  Source: {input_img_id}')
        # logging.debug(f'  Bucket TIF:  {bucket_img}')
        # logging.debug(f'  Bucket JSON: {bucket_json}')

        export_id = (
            f'reference_et_{region.lower().replace("/", "_")}_{timestep.lower()}_'
            f'{version.lower()}_{image_id}_bucket_export'
        )
        logging.debug(f'  Export ID: {export_id}')

        if tasks and (export_id in tasks.keys()):
            logging.debug(f'  {image_id} - Task already submitted, skipping')
            continue
        if bucket_files and (bucket_img in bucket_files):
            logging.info(f'  {image_id} - Image is in bucket, skipping')
            continue

        image_info = input_asset_props[image_id]

        # TODO: Wrap in try/except loop
        task = ee.batch.Export.image.toCloudStorage(
            image=ee.Image(input_img_id).toFloat(),
            description=export_id,
            bucket=bucket_name,
            fileNamePrefix=bucket_img.replace('.tif', ''),
            **export_params,
        )

        if not task:
            logging.warning(f'  {image_id} - Export task was not built, skipping')
            continue

        logging.info(f'  {image_id} - Starting export task')
        max_retries = 4
        for i in range(1, max_retries):
            try:
                task.start()
                break
            except Exception as e:
                logging.info(f'  Resending task start ({i}/{max_retries})')
                logging.debug(f'  {e}')
                time.sleep(i ** 2)
        # # Not using ee_task_start since it doesn't return the task object
        # utils.ee_task_start(task)

        # if export_properties_json and (bucket_json not in bucket_files):
        if export_properties_json:
            logging.debug(f'  {image_id} - Writing properties JSON to bucket')

            # # Remove unneeded properties
            # for k in ['system:footprint']:
            #     if k in scene_info['properties'].keys():
            #         del scene_info[k]

            max_retries = 4
            for i in range(1, max_retries):
                try:
                    bucket = storage_client.bucket(bucket_name)
                    blob = bucket.blob(bucket_json)
                    blob.upload_from_string(json.dumps(image_info['properties'], sort_keys=True))
                    break
                except Exception as e:
                    logging.info(f'  Retrying blob upload ({i}/{max_retries})')
                    logging.debug(f'  {e}')
                    time.sleep(i ** 3)

        # Pause before starting the next export task
        ready_task_count += 1
        ready_task_count = utils.delay_task(
            delay_time=delay_time,
            task_max=ready_task_max,
            task_count=ready_task_count,
        )


def arg_parse():
    """"""
    parser = argparse.ArgumentParser(
        description='Export reference ET assets to bucket',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--timestep', choices=['daily', 'monthly'], metavar='TIME', required=True,
        help=f'Timestep (choices:{", ".join(TIMESTEPS)})')
    parser.add_argument(
        '--region', choices=REGIONS, metavar='REGION', default='conus/gridmet',
        help=f'Region/dataset name (choices:{", ".join(REGIONS)})')
    # parser.add_argument(
    #     '--version', choices=VERSIONS, metavar='VERSIONS', default='v1',
    #     help=f'Version (choices:{", ".join(VERSIONS)})')
    parser.add_argument(
        '--start', required=True, type=utils.arg_valid_date, metavar='YYYY-MM-DD',
        help='Start date')
    parser.add_argument(
        '--end', required=True, type=utils.arg_valid_date, metavar='YYYY-MM-DD',
        help='End date (exclusive)')
    parser.add_argument(
        '--project', required=True, help='Google cloud project ID')
    parser.add_argument(
        '--bucket', required=True, help='Google cloud storage bucket name')
    parser.add_argument(
        '--delay', default=0, type=float,
        help='Delay (in seconds) between each export tasks')
    parser.add_argument(
        '--key', type=utils.arg_valid_file, metavar='FILE',
        help='Earth Engine service account JSON key file to use for GEE initialization')
    parser.add_argument(
        '--ready', default=-1, type=int,
        help='Maximum number of queued READY tasks')
    parser.add_argument(
        '--reverse', default=False, action='store_true',
        help='Process dates in reverse order')
    parser.add_argument(
        '--debug', default=logging.INFO, const=logging.DEBUG,
        help='Debug level logging', action='store_const', dest='loglevel')
    args = parser.parse_args()

    return args


if __name__ == "__main__":
    args = arg_parse()
    logging.basicConfig(level=args.loglevel, format='%(message)s')

    main(
        timestep=args.timestep,
        region=args.region,
        # version=args.version,
        start_dt=args.start,
        end_dt=args.end,
        project_id=args.project,
        bucket_name=args.bucket,
        delay_time=args.delay,
        ready_task_max=args.ready,
        reverse_flag=args.reverse,
        gee_key_file=args.key,
    )
