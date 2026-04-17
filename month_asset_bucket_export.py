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

MODELS = [
    'DISALEXI', 'EEMETRIC', 'GEESEBAL', 'PTJPL', 'SIMS', 'SSEBOP',
    'ENSEMBLE',
    'NDVI',
]
REGIONS = ['conus/gridmet', 'california/cimis']
VERSIONS = ['v2_0', 'v2_1']


def main(
        model_name,
        region,
        version,
        start_dt,
        end_dt,
        bucket_name,
        project_id,
        delay_time=0,
        ready_task_max=-1,
        mgrs_tiles=None,
        export_properties_json=True,
        reverse_flag=False,
        gee_key_file=None,
):
    """Export OpenET monthly ET assets to bucket

    Parameters
    ----------
    model_name : str
    region : str
    version : str
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
    mgrs_tiles : str, optional
        Comma separated UTM zones or MGRS tiles to process (the default is None).
    export_properties_json : bool, optional
        Export a properties JSON file for each image.
    reverse_flag : bool, optional
        If True, process WRS2 tiles in reverse order (the default is False).
    gee_key_file : str, None, optional
        Earth Engine service account JSON key file (the default is None).
        If set, this will be used instead of the cloud project ID for
        initializing/authenticating GEE.

    """
    logging.info(f'\nExport {model_name} {region} month assets to bucket')

    start_date = start_dt.strftime('%Y-%m-%d')
    end_date = end_dt.strftime('%Y-%m-%d')
    logging.info(f'  Start: {start_date}')
    logging.info(f'  End:   {end_date}')

    mgrs_skip_list = []
    # date_skip_list = []

    # Default datatype and nodata value
    dtype = 'uint16'
    nodata = 65535

    # Model specific inputs
    if model_name.upper() in ['NDVI']:
        # Override the version and region for NDVI exports since the data is global by default
        # and the version number is fixed (and different than the ET data)
        version = 'v2_1'
        region = 'global'
        input_coll_id = f'projects/openet/assets/{model_name.lower()}/{region.lower()}/monthly/{version.lower()}'
        bucket_folder = f'{model_name.lower()}/{region.lower()}/monthly/{version.lower()}'
        dtype = 'int16'
        nodata = -32768
    # elif model_name.upper() == 'ENSEMBLE':
    else:
        input_coll_id = f'projects/openet/assets/{model_name.lower()}/{region.lower()}/monthly/{version.lower()}'
        bucket_folder = f'{model_name.lower()}/{region.lower()}/monthly/{version.lower()}'

    if region == 'california/cimis':
        mgrs_ftr_coll_id = 'projects/openet/assets/mgrs/california/cimis/zones'
    else:
        mgrs_ftr_coll_id = 'projects/openet/assets/mgrs/conus/gridmet/zones'

    if mgrs_tiles:
        mgrs_tiles = sorted([x.strip() for x in mgrs_tiles.split(',')])
        mgrs_tiles = [x.upper() for x in mgrs_tiles if x]
        logging.info(f'  mgrs_tiles: {", ".join(mgrs_tiles)}')
        utm_zones = sorted(list(set([int(x[:2]) for x in mgrs_tiles])))
        logging.info(f'  utm_zones:  {", ".join(map(str, utm_zones))}')
    else:
        mgrs_tiles = []
        utm_zones = []

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

    ee.data.setWorkloadTag(f'{model_name.lower()}-month-bucket-export')

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


    # Get list of MGRS tiles that intersect the study area
    logging.debug('\nMGRS Tiles/Zones')
    export_list = mgrs_export_tiles(
        mgrs_coll_id=mgrs_ftr_coll_id,
        mgrs_tiles=mgrs_tiles,
        mgrs_skip_list=mgrs_skip_list,
        utm_zones=utm_zones,
    )
    if not export_list:
        logging.error('\nEmpty export list, exiting')
        return False

    # Process each WRS2 tile separately
    logging.info('\nImage Exports')
    for export_info in sorted(export_list, key=lambda i: i['index'], reverse=reverse_flag):
        mgrs_tile = export_info['index'].upper()
        logging.info(f'MGRS Tile: {mgrs_tile}')
        # logging.info(f'MGRS Tile: {mgrs_tile} ({export_n + 1}/{len(export_list)})')
        # logging.info(f'{export_info["index"]}')

        logging.debug(f'  Shape:      {export_info["shape_str"]}')
        logging.debug(f'  Transform:  {export_info["geo_str"]}')
        logging.debug(f'  Extent:     {export_info["extent"]}')
        logging.debug(f'  MaxPixels:  {export_info["maxpixels"]}')

        # logging.debug('  {} - {}'.format(
        #     export_info['index'], ", ".join(export_info['wrs2_tiles'])
        # ))
        # tile_geom = ee.Geometry.Rectangle(export_info['extent'], export_info['crs'], False)

        # Get the available image ID list for the mgrs tile
        logging.debug('  Getting list of available input/output assets')
        logging.debug(f'  {input_coll_id}')
        input_coll = (
            ee.ImageCollection(input_coll_id)
            .filterDate(start_dt, end_dt)
            .filterMetadata('mgrs_tile', 'equals', export_info["index"])
        )
        input_asset_props = {
            f'{ftr["properties"]["system:index"]}': ftr
            for ftr in utils.get_info(input_coll)['features']
        }
        input_id_list = input_asset_props.keys()

        if not input_id_list:
            logging.info('  No source images in date range, skipping zone')
            continue
        # image_id_list = sorted(image_id_list, key=lambda k: k.split('_')[-1], reverse=reverse_flag)

        for image_id in input_id_list:
            logging.info(f'{image_id}')

            image_start_date = image_id.split('_')[1]
            # mgrs_tile, image_start_date, image_end_date = image_id.split('_')
            input_img_id = f'{input_coll_id}/{image_id}'
            # output_img_id = f'{output_coll_id}/{image_id}'
            bucket_img = f'{bucket_folder}/{image_id}.tif'
            bucket_json = f'{bucket_folder}/{image_id}_properties.json'
            logging.debug(f'  Source: {input_img_id}')
            # logging.debug(f'  Destination: {output_img_id}')
            # logging.debug(f'  Bucket TIF:  {bucket_img}')
            # logging.debug(f'  Bucket JSON: {bucket_json}')

            export_id = (
                f'{model_name.lower()}_{region.lower().replace("/", "_")}_monthly_'
                f'{version.lower()}_{mgrs_tile.lower()}_{image_start_date}_bucket_export'
            )
            logging.debug(f'  Export ID: {export_id}')

            # TODO: If overwrite support is ever added,
            #   make sure to delete the asset first before removing the bucket file
            #   or starting a new export

            if tasks and (export_id in tasks.keys()):
                logging.debug(f'  {image_id} - Task already submitted, skipping')
                continue
            if bucket_files and (bucket_img in bucket_files):
                logging.info(f'  {image_id} - Image is in bucket, skipping')
                continue

            image_info = input_asset_props[image_id]

            input_img = ee.Image(input_img_id)

            # Force the output to a single data type for all bands
            #   since COGs must have the same datatype for all bands
            if dtype == 'uint16':
                output_img = input_img.clamp(0, 65535).uint16()
            elif dtype == 'int16':
                output_img = input_img.clamp(-32768, 32767).int16()
            # elif dtype == 'float32':
            #     output_img = input_img.unmask(nodata)
            else:
                raise ValueError('Only uint16 dtypes are currently supported')

            # Extra masking calculations are needed to get around the bug with
            #   integer exports where masked pixels are sometimes set to 0
            if dtype in ['uint16', 'int16']:
                nodata_mask = output_img.mask().lte(0)
                output_img = (
                    nodata_mask.multiply(nodata)
                    .where(nodata_mask.eq(0), output_img)
                    .rename(output_img.bandNames())
                )

            # TODO: Wrap in try/except loop
            task = ee.batch.Export.image.toCloudStorage(
                image=output_img,
                description=export_id,
                bucket=bucket_name,
                fileNamePrefix=bucket_img.replace('.tif', ''),
                # TODO: Should these be read from the image instead?
                dimensions=export_info['shape_str'],
                crs=export_info['crs'],
                crsTransform=export_info['geo_str'],
                # dimensions=image_info['bands'][0]['dimensions'],
                # crs=image_info['bands'][0]['crs'],
                # crsTransform=image_info['bands'][0]['crs_transform'],
                maxPixels=int(1E12),
                fileDimensions=[32768, 32768],
                fileFormat='GeoTIFF',
                formatOptions={'cloudOptimized': True, 'noData': nodata},
                # pyramidingPolicy='mean',
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
                delay_time=delay_time, task_max=ready_task_max, task_count=ready_task_count,
            )


# CGM - This is a simplified version of the openet.core.export.mgrs_export_tiles()
#   that doesn't require the study area collection ID to be set
#   and has the wrs2 tiles filtering removed
def mgrs_export_tiles(
        mgrs_coll_id,
        mgrs_tiles=[],
        mgrs_skip_list=[],
        utm_zones=[],
        mgrs_property='mgrs',
        utm_property='utm',
        cell_size=30,
):
    """Select MGRS tiles and metadata

    Parameters
    ----------
    mgrs_coll_id : str
        MGRS feature collection asset ID.
    mgrs_tiles : list, optional
        User defined MGRS tile subset.
    mgrs_skip_list : list, optional
        User defined list MGRS tiles to skip.
    utm_zones : list, optional
        User defined UTM zone subset.
    mgrs_property : str, optional
        MGRS property in the MGRS feature collection (the default is 'mgrs').
    utm_property : str, optional
        UTM zone property in the MGRS feature collection (the default is 'utm').
    cell_size : float, optional
        Cell size for transform and shape calculation (the default is 30).

    Returns
    ------
    list of dicts: export information

    """
    logging.debug('Building MGRS tile list')
    tiles_coll = ee.FeatureCollection(mgrs_coll_id)

    # Filter collection by user defined lists
    if utm_zones:
        logging.debug(f'  Filter user UTM Zones:    {utm_zones}')
        tiles_coll = tiles_coll.filter(ee.Filter.inList(utm_property, utm_zones))
    if mgrs_skip_list:
        logging.debug(f'  Filter MGRS skip list:    {mgrs_skip_list}')
        tiles_coll = tiles_coll.filter(
            ee.Filter.inList(mgrs_property, mgrs_skip_list).Not()
        )
    if mgrs_tiles:
        logging.debug(f'  Filter MGRS tiles/zones:  {mgrs_tiles}')
        # Allow MGRS tiles to be subsets of the full tile code
        #   i.e. mgrs_tiles = 10TE, 10TF
        mgrs_filters = [
            ee.Filter.stringStartsWith(mgrs_property, mgrs_id.upper())
            for mgrs_id in mgrs_tiles
        ]
        tiles_coll = tiles_coll.filter(ee.call('Filter.or', mgrs_filters))

    # Drop the MGRS tile geometry to simplify the getInfo call
    def drop_geometry(ftr):
        return ee.Feature(None).copyProperties(ftr)

    logging.debug('  Requesting tile/zone info')
    tiles_info = utils.get_info(tiles_coll.map(drop_geometry))

    # Constructed as a list of dicts to mimic other interpolation/export tools
    tiles_list = []
    for tile_ftr in tiles_info['features']:
        mgrs_id = tile_ftr['properties']['mgrs'].upper()
        tile_extent = [
            int(tile_ftr['properties']['xmin']),
            int(tile_ftr['properties']['ymin']),
            int(tile_ftr['properties']['xmax']),
            int(tile_ftr['properties']['ymax'])
        ]
        tile_geo = [cell_size, 0, tile_extent[0], 0, -cell_size, tile_extent[3]]
        tile_shape = [
            int((tile_extent[2] - tile_extent[0]) / cell_size),
            int((tile_extent[3] - tile_extent[1]) / cell_size)
        ]
        tiles_list.append({
            'crs': 'EPSG:{:d}'.format(int(tile_ftr['properties']['epsg'])),
            'extent': tile_extent,
            'geo': tile_geo,
            'geo_str': '[' + ','.join(map(str, tile_geo)) + ']',
            'index': mgrs_id,
            'maxpixels': tile_shape[0] * tile_shape[1] + 1,
            'shape': tile_shape,
            'shape_str': '{0}x{1}'.format(*tile_shape),
            'utm': int(mgrs_id[:2]),
        })

    export_list = [tile for tile in sorted(tiles_list, key=lambda k: k['index'])]

    return export_list


def arg_parse():
    """"""
    parser = argparse.ArgumentParser(
        description='Export month assets to bucket',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--model', required=True, choices=MODELS, metavar='MODEL',
        help=f'OpenET model name (choices:{", ".join(MODELS)})')
    parser.add_argument(
        '--region', choices=REGIONS, metavar='REGION', default='conus/gridmet',
        help=f'Region/dataset name (choices:{", ".join(REGIONS)})')
    parser.add_argument(
        '--version', choices=VERSIONS, metavar='VERSIONS', default='v2_1',
        help=f'Version (choices:{", ".join(VERSIONS)})')
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
        '--tiles', default='',
        help='Comma separated list of UTM zones or MGRS grid zones to process')
    parser.add_argument(
        '--debug', default=logging.INFO, const=logging.DEBUG,
        help='Debug level logging', action='store_const', dest='loglevel')
    args = parser.parse_args()

    return args


if __name__ == "__main__":
    args = arg_parse()
    logging.basicConfig(level=args.loglevel, format='%(message)s')

    main(
        model_name=args.model,
        region=args.region,
        version=args.version,
        start_dt=args.start,
        end_dt=args.end,
        project_id=args.project,
        bucket_name=args.bucket,
        mgrs_tiles=args.tiles,
        delay_time=args.delay,
        ready_task_max=args.ready,
        reverse_flag=args.reverse,
        gee_key_file=args.key,
    )
