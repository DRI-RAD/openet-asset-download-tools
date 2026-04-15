import argparse
from collections import defaultdict
import json
import logging
import os
import re
import time

import ee
from bqplot import Lines
from google.cloud import storage

import openet.core.utils as utils

logging.getLogger('earthengine-api').setLevel(logging.INFO)
logging.getLogger('googleapiclient').setLevel(logging.INFO)
logging.getLogger('requests').setLevel(logging.INFO)
logging.getLogger('urllib3').setLevel(logging.INFO)

MODELS = [
    'DISALEXI', 'EEMETRIC', 'GEESEBAL', 'PTJPL', 'SIMS', 'SSEBOP',
    'ENSEMBLE',
]
REGIONS = ['conus/gridmet', 'california/cimis']


def main(
        model_name,
        start_dt,
        end_dt,
        bucket_name,
        project_id,
        delay_time=0,
        ready_task_max=-1,
        mgrs_tiles=None,
        wrs2_tiles=None,
        export_properties_json=True,
        reverse_flag=False,
        gee_key_file=None,
):
    """

    Parameters
    ----------
    model_name : str
        OpenET model name.
    bucket_name : str
        Google Cloud Storage bucket name.
    project_id : str
        Google Cloud project ID.
    start_dt : datetime
        Start date.
    end_dt : datetime
        End date (inclusive).
    delay_time : float, optional
        Delay time in seconds between starting export tasks (or checking the
        number of queued tasks, see "ready_task_max" parameter).
        The default is 0.
    ready_task_max : int, optional
        Maximum number of queued "READY" tasks.  The default is -1 which is
        implies no limit to the number of tasks that will be submitted.
    mgrs_tiles : str, optional
        Comma separated UTM zones or MGRS tiles to process (the default is None).
    wrs2_tiles : str, optional
        Comma separated WRS2 tiles to process (the default is None).
    export_properties_json : bool, optional
        Export a properties JSON file for each image
    reverse_flag : bool, optional
        If True, process WRS2 tiles in reverse order (the default is False).
    gee_key_file : str, None, optional
        Earth Engine service account JSON key file (the default is None).
        If set, this will be used instead of the cloud project ID for
        initializing/authenticating GEE.

    """
    logging.info(f'\nExport {model_name} scene assets to bucket')

    start_date = start_dt.strftime('%Y-%m-%d')
    end_date = end_dt.strftime('%Y-%m-%d')
    logging.info(f'  Start: {start_date}')
    logging.info(f'  End:   {end_date}')

    wrs2_property = 'wrs2_tile'
    wrs2_tile_re = re.compile('p?(\d{1,3})r?(\d{1,3})')

    # List of path/rows to skip
    wrs2_skip_list = [
        'p049r026',  # Vancouver Island, Canada
        'p048r028',  # OR/WA Coast
        # 'p047r031',  # North California coast
        'p042r037',  # San Nicholas Island, California
        # 'p041r037', # South California coast
        # 'p040r038', 'p039r038', 'p038r038',  # Mexico (by California)
        'p037r039', 'p036r039', 'p035r039',  # Mexico (by Arizona)
        'p034r039', 'p033r039', # Mexico (by New Mexico)
        'p032r040',  # Mexico (West Texas)
        'p029r041', 'p028r042', 'p027r043', 'p026r043',  # Mexico (South Texas)
        'p019r040', 'p018r040',  # West Florida coast
        'p016r043', 'p015r043',  # South Florida coast
        'p014r041', 'p014r042', 'p014r043',  # East Florida coast
        'p013r035', 'p013r036',  # North Carolina Outer Banks
        'p013r026', 'p012r026',  # Canada (by Maine)
        'p011r032',  # Rhode Island coast
    ]
    wrs2_path_skip_list = [9, 49]
    wrs2_row_skip_list = [25, 24, 43]
    mgrs_skip_list = []
    # date_skip_list = []

    # TODO: Define a better structure for the scene ID skip list
    #   Add a regex to check for valid scene IDs
    #   For now assume each line has one valid scene ID
    scene_id_skip_list = []
    scene_skip_list_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), 'scene_skip_list.csv')
    if os.path.isfile(scene_skip_list_path):
        with open(scene_skip_list_path) as scene_f:
            for line in scene_f.readlines()[1:]:
                scene_id = line.split(',')[0].strip()
                scene_id_skip_list.append(scene_id)

    # Default datatype and nodata value
    dtype = 'uint16'
    nodata = 65535

    # Model specific inputs
    if model_name.upper() == 'ENSEMBLE':
        input_coll_id = f'projects/openet/assets/{model_name.lower()}/conus/gridmet/landsat/v2_1'
        bucket_folder = f'{model_name.lower()}/conus/gridmet/landsat/v2_1'
    elif model_name.upper() == 'DISALEXI':
        input_coll_id = f'projects/openet/assets/{model_name.lower()}/conus/cfsr/landsat/v2_1'
        bucket_folder = f'{model_name.lower()}/conus/cfsr/landsat/v2_1'
    elif model_name.upper() == 'PTJPL':
        input_coll_id = f'projects/openet/assets/{model_name.lower()}/conus/nldas2/landsat/v2_1'
        bucket_folder = f'{model_name.lower()}/conus/nldas2/landsat/v2_1'
    # elif model_name.upper() == 'NDVI':
    #     input_coll_id = f'projects/openet/assets/sims/conus/gridmet/landsat/v2_1'
    #     bucket_folder = f'{model_name.lower()}/conus/gridmet/landsat/v2_1'
    else:
        input_coll_id = f'projects/openet/assets/{model_name.lower()}/conus/gridmet/landsat/v2_1'
        bucket_folder = f'{model_name.lower()}/conus/gridmet/landsat/v2_1'

    mgrs_ftr_coll_id = 'projects/openet/assets/mgrs/conus/gridmet/zones'
    # if region == 'california/cimis':
    #     mgrs_ftr_coll_id = 'projects/openet/assets/mgrs/california/cimis/zones'
    # else:
    #     mgrs_ftr_coll_id = 'projects/openet/assets/mgrs/conus/gridmet/zones'

    if model_name.upper() in ['ENSEMBLE']:
        input_bands = ['et_ensemble_mad', 'et_ensemble_mad_count']
        output_bands = ['et_ensemble_mad', 'et_ensemble_mad_count']
        # input_bands = ['et_ensemble_mad', 'et_ensemble_mad_count']
        # output_bands = ['et', 'model_count']
    elif model_name.upper() in ['NDVI']:
        input_bands = ['ndvi']
        output_bands = ['ndvi']
    # elif model_name.upper() in ['EEMETRIC', 'SIMS', 'SSEBOP']:
    #     input_bands = ['et', 'et_fraction']
    #     output_bands = ['et', 'et_fraction']
    else:
        input_bands = ['et']
        output_bands = ['et']

    if mgrs_tiles:
        mgrs_tiles = sorted([x.strip() for x in mgrs_tiles.split(',')])
        mgrs_tiles = [x.upper() for x in mgrs_tiles if x]
        logging.info(f'  mgrs_tiles: {", ".join(mgrs_tiles)}')
        utm_zones = sorted(list(set([int(x[:2]) for x in mgrs_tiles])))
        logging.info(f'  utm_zones:  {", ".join(map(str, utm_zones))}')
    else:
        mgrs_tiles = []
        utm_zones = []

    if wrs2_tiles:
        wrs2_tiles = sorted([x.strip() for x in wrs2_tiles.split(',')])
        wrs2_tiles = [x.lower() for x in wrs2_tiles if x]
        logging.info(f'  wrs2_tiles: {", ".join(wrs2_tiles)}')
    else:
        wrs2_tiles = []

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

    ee.data.setWorkloadTag(f'{model_name.lower()}-scene-bucket-export')

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
        wrs2_tiles=wrs2_tiles,
    )
    if not export_list:
        logging.error('\nEmpty export list, exiting')
        return False

    # Process each WRS2 tile separately
    logging.info('\nImage Exports')
    processed_scene_ids = set()
    for export_info in sorted(export_list, key=lambda i: i['index'], reverse=reverse_flag):
        logging.info(f'{export_info["index"]}')
        logging.debug(f'  {", ".join(export_info["wrs2_tiles"])}')
        tile_count = len(export_info['wrs2_tiles'])
        tile_list = sorted(export_info['wrs2_tiles'], reverse=not(reverse_flag))

        # Get the available image ID list for the zone
        # Get list of existing image assets and their properties for the zone
        # Process date range by years to avoid requesting more than 3000 images
        logging.debug('  Getting list of available input/output assets')
        input_scene_id_list = []
        input_asset_props = {}
        for year_start_dt, year_end_dt in utils.date_years(start_dt, end_dt, exclusive_end_dates=True):
            year_start_date = year_start_dt.strftime("%Y-%m-%d")
            year_end_date = year_end_dt.strftime("%Y-%m-%d")
            logging.debug(f'  {year_start_date} {year_end_date}')

            # Just filter to the wrs2 tile list for now
            input_coll = (
                ee.ImageCollection(input_coll_id)
                .filterDate(year_start_date, year_end_date)
                .filter(ee.Filter.inList(wrs2_property, tile_list))
            )

            year_asset_props = {
                f'{ftr["properties"]["system:index"]}': ftr
                for ftr in utils.get_info(input_coll)['features']
            }
            input_asset_props.update(year_asset_props)

            # Filter image_ids that have already been processed as part of a
            #   different MGRS tile (might be faster with sets)
            year_input_id_list = [x for x in year_asset_props.keys() if x not in processed_scene_ids]

            # Keep track of all the image_ids that have been processed
            processed_scene_ids.update(year_input_id_list)
            input_scene_id_list.extend(year_input_id_list)

        if not input_scene_id_list:
            logging.info('  No source images in date range, skipping zone')
            continue
        input_scene_id_list = sorted(
            input_scene_id_list, key=lambda k: k.split('_')[-1], reverse=reverse_flag
        )

        # Group images by wrs2 tile
        scene_id_lists = defaultdict(list)
        for scene_id in input_scene_id_list:
            wrs2_tile = 'p{}r{}'.format(*wrs2_tile_re.findall(scene_id.split('_')[1])[0])
            if wrs2_tile not in tile_list:
                continue
            scene_id_lists[wrs2_tile].append(scene_id)

        for export_n, wrs2_tile in enumerate(tile_list):
            path, row = map(int, wrs2_tile_re.findall(wrs2_tile)[0])

            if wrs2_skip_list and (wrs2_tile in wrs2_skip_list):
                logging.debug('{} {} ({}/{}) - in wrs2 skip list'.format(
                    export_info['index'], wrs2_tile, export_n + 1, tile_count))
                continue
            elif wrs2_row_skip_list and (row in wrs2_row_skip_list):
                logging.debug('{} {} ({}/{}) - in wrs2 row skip list'.format(
                    export_info['index'], wrs2_tile, export_n + 1, tile_count))
                continue
            elif wrs2_path_skip_list and (path in wrs2_path_skip_list):
                logging.debug('{} {} ({}/{}) - in wrs2 path skip list'.format(
                    export_info['index'], wrs2_tile, export_n + 1, tile_count))
                continue
            else:
                logging.debug('{} {} ({}/{})'.format(
                    export_info['index'], wrs2_tile, export_n + 1, tile_count))
            wrs2_tiles.append(wrs2_tile)

            # Subset the image ID list to the WRS2 tile
            try:
                scene_id_list = scene_id_lists[wrs2_tile]
            except KeyError:
                scene_id_list = []
            if not scene_id_list:
                logging.debug('  No Landsat images in date range, skipping tile')
                continue

            for scene_id in scene_id_list:
                logging.info(f'{scene_id}')

                input_img_id = f'{input_coll_id}/{scene_id}'
                bucket_img = f'{bucket_folder}/{scene_id}.tif'
                bucket_json = f'{bucket_folder}/{scene_id}_properties.json'
                export_id = f'{model_name.lower()}_{scene_id}_bucket_export'
                logging.debug(f'  Source: {input_img_id}')

                # TODO: If overwrite support is ever added,
                #   make sure to delete the asset first before removing the bucket file
                #   or starting a new export

                if tasks and (export_id in tasks.keys()):
                    logging.debug(f'  {scene_id} - Task already submitted, skipping')
                    continue
                if bucket_files and (bucket_img in bucket_files):
                    logging.info(f'  {scene_id} - Image is in bucket, skipping')
                    continue

                image_info = input_asset_props[scene_id]

                input_img = ee.Image(input_img_id).select(input_bands, output_bands)

                # Force the output to a single data type for all bands
                #   since COGs must have the same datatype for all bands
                if dtype == 'uint16':
                    output_img = input_img.clamp(0, 65534).uint16()
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
                    image=input_img,
                    description=export_id,
                    bucket=bucket_name,
                    fileNamePrefix=bucket_img.replace('.tif', ''),
                    dimensions=image_info['bands'][0]['dimensions'],
                    crs=image_info['bands'][0]['crs'],
                    crsTransform=image_info['bands'][0]['crs_transform'],
                    maxPixels=int(1E10),
                    fileFormat='GeoTIFF',
                    formatOptions={'cloudOptimized': True, 'noData': nodata},
                    # pyramidingPolicy='mean',
                )

                if not task:
                    logging.warning(f'  {scene_id} - Export task was not built, skipping')
                    continue

                logging.info(f'  {scene_id} - Starting export task')
                max_retries = 4
                for i in range(1, max_retries):
                    try:
                        task.start()
                        break
                    except Exception as e:
                        logging.info(f'  Resending task start ({i}/{4})')
                        logging.debug(f'  {e}')
                        time.sleep(i ** 2)
                # # Not using ee_task_start since it doesn't return the task object
                # utils.ee_task_start(task)

                if export_properties_json and (bucket_json not in bucket_files):
                    logging.debug(f'  {scene_id} - Writing properties JSON to bucket')

                    # # Remove unneeded properties
                    # for k in ['system:footprint']:
                    #     if k in scene_info['properties'].keys():
                    #         del scene_info[k]

                    for i in range(1, max_retries):
                        try:
                            bucket = storage_client.bucket(bucket_name)
                            blob = bucket.blob(bucket_json)
                            blob.upload_from_string(
                                json.dumps(image_info['properties'], sort_keys=True)
                            )
                            break
                        except Exception as e:
                            logging.info(f'  Retrying blob upload ({i}/{4})')
                            logging.debug(f'  {e}')
                            time.sleep(i ** 3)

                # Pause before starting the next export task
                ready_task_count += 1
                ready_task_count = utils.delay_task(
                    delay_time=delay_time, task_max=ready_task_max, task_count=ready_task_count,
                )


# CGM - This is a simplified version of the openet.core.export.mgrs_export_tiles()
#   that doesn't require the study area collection ID to be set
def mgrs_export_tiles(
        mgrs_coll_id,
        mgrs_tiles=[],
        mgrs_skip_list=[],
        utm_zones=[],
        wrs2_tiles=[],
        mgrs_property='mgrs',
        utm_property='utm',
        wrs2_property='wrs2',
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
    wrs2_property : str, optional
        WRS2 property in the MGRS feature collection (the default is 'wrs2').
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
            'wrs2_tiles': sorted(utils.wrs2_str_2_set(tile_ftr['properties'][wrs2_property])),
        })

    # Apply the user defined WRS2 tile list
    if wrs2_tiles:
        logging.debug(f'  Filter WRS2 tiles: {wrs2_tiles}')
        for tile in tiles_list:
            tile['wrs2_tiles'] = sorted(list(set(tile['wrs2_tiles']) & set(wrs2_tiles)))

    export_list = [tile for tile in sorted(tiles_list, key=lambda k: k['index'])]

    return export_list


def arg_parse():
    """"""
    parser = argparse.ArgumentParser(
        description='Export scene assets to bucket',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--model_name', choices=MODELS, metavar='MODEL',
        help=f'ET model_name name (choices:{", ".join(MODELS)})')
    parser.add_argument(
        '--start', required=True, type=utils.arg_valid_date, metavar='DATE',
        help='Start date (format YYYY-MM-DD)')
    parser.add_argument(
        '--end', required=True, type=utils.arg_valid_date, metavar='DATE',
        help='End date (format YYYY-MM-DD)')
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
        '--wrs2', default='',
        help='Comma separated list of WRS2 tiles to process')
    parser.add_argument(
        '--debug', default=logging.INFO, const=logging.DEBUG,
        help='Debug level logging', action='store_const', dest='loglevel')
    args = parser.parse_args()

    return args


if __name__ == "__main__":
    args = arg_parse()
    logging.basicConfig(level=args.loglevel, format='%(message)s')

    main(
        model_name=args.model_name,
        start_dt=args.start,
        end_dt=args.end,
        project_id=args.project,
        bucket_name=args.bucket,
        mgrs_tiles=args.tiles,
        wrs2_tiles=args.wrs2,
        delay_time=args.delay,
        ready_task_max=args.ready,
        reverse_flag=args.reverse,
        gee_key_file=args.key,
    )
