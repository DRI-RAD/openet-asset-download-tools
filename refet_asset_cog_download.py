import argparse
import json
import logging
import os

import ee
import numpy as np
import rasterio
import rasterio.shutil
import xee
import xarray

import openet.core.utils as utils

logging.getLogger('earthengine-api').setLevel(logging.WARNING)
logging.getLogger('googleapiclient').setLevel(logging.ERROR)
logging.getLogger('rasterio').setLevel(logging.INFO)
logging.getLogger('requests').setLevel(logging.WARNING)
logging.getLogger('xee').setLevel(logging.INFO)
logging.getLogger('xarray').setLevel(logging.INFO)
logging.getLogger('urllib3').setLevel(logging.WARNING)

TIMESTEPS = ['daily', 'monthly']
REGIONS = ['conus/gridmet', 'california/cimis']


def main(
        timestep,
        region,
        start_dt,
        end_dt,
        project_id,
        workspace,
        export_properties_json=True,
        overwrite_flag=False,
        reverse_flag=False,
        gee_key_file=None,
        cleanup=True,
        workers=10,
):
    """Download OpenET monthly reference ET assets to COG

    Parameters
    ----------
    timestep : {'monthly', 'daily'}
        Data timestep.
    region : {'conus/gridmet', 'california/cimis'}
        Reference ET dataset region .
    project_id : str
        Google Cloud project ID to use for GEE initialization.
    workspace : str
        Root folder where the images will be saved.
    start_dt : datetime
        Start date.
    end_dt : datetime
        End date (exclusive).
    export_properties_json : bool, optional
        Export a properties JSON file for each image.
    overwrite_flag : bool, optional
        If True, overwrite existing files (the default is False).
    reverse_flag : bool, optional
        If True, process WRS2 tiles in reverse order (the default is False).
    gee_key_file : str, None, optional
        Earth Engine service account JSON key file (the default is None).
        If set, this will be used instead of the cloud project ID for
        initializing/authenticating GEE.
    cleanup : bool, optional,
        If True, remove temporary files
    workers : int, optional
        The number of workers to use in the xarray call (the default is 10).

    """
    logging.info(f'\nDownload reference ET {region} {timestep} assets to COG')

    start_date = start_dt.strftime('%Y-%m-%d')
    end_date = end_dt.strftime('%Y-%m-%d')
    logging.info(f'  Start: {start_date}')
    logging.info(f'  End:   {end_date}')

    # Set the export parameters based on the region
    # Hardcoding for now but these could be read dynamically from the input collection
    if region.lower() == 'conus/gridmet':
        version = 'v1'
        input_coll_id = f'projects/openet/assets/reference_et/{region.lower()}/{timestep.lower()}/{version}'
        output_folder = f'{workspace}/reference_et/{region.lower()}/{timestep.lower()}/{version}'
        input_bands = ['eto', 'etr']
        output_bands = ['eto', 'etr']
        crs = 'EPSG:4326'
        shape = [1386, 585]
        crs_transform = (0.041666666666666664, 0, -124.7875, 0, -0.041666666666666664, 49.42083333333334)
        nodata = -9999
        dtype = 'float32'
    elif region.lower() == 'california/cimis':
        version = 'v1'
        input_coll_id = f'projects/openet/assets/reference_et/{region.lower()}/{timestep.lower()}/{version}'
        output_folder = f'{workspace}/reference_et/{region.lower()}/{timestep.lower()}/{version}'
        input_bands = ['eto', 'etr']
        output_bands = ['eto', 'etr']
        # NAD_1983_California_Teale_Albers
        crs = 'EPSG:3310'
        # crs = rasterio.crs.CRS.from_proj4(
        #     '+proj=aea +lat_1=34 +lat_2=40.5 +lat_0=0 +lon_0=-120 ' +
        #     '+x_0=0 +y_0=-4000000 +ellps=GRS80 +datum=NAD83 +units=m +no_defs'
        # )
        # crs = crs.to_wkt()
        shape = [510, 560]
        crs_transform = (2000, 0, -410000.0, 0, -2000, 460000.0)
        nodata = -9999
        dtype = 'float32'
    else:
        raise ValueError(f'Unsupported region: {region}')

    if not os.path.isdir(output_folder):
        os.makedirs(output_folder)

    logging.debug(f'  Shape:      {shape}')
    logging.debug(f'  Transform:  {crs_transform}')
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
        f'reference_et-{region.lower().replace("/", "_")}-{timestep.lower()}-cog-download'
    )

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

        image_info = input_asset_props[image_id]

        input_img_id = f'{input_coll_id}/{image_id}'
        logging.debug(f'  Source: {input_img_id}')

        # TODO: Write temp image to a temporary (or in memory) workspace
        temp_path = f'{output_folder}/{image_id}_temp.tif'
        # temp_path = f'{temp_folder}/{image_id}_temp.tif'

        tif_path = f'{output_folder}/{image_id}.tif'
        json_path = f'{output_folder}/{image_id}_properties.json'
        # tif_path = f'{output_folder}/{year}/{image_id}.tif'
        # json_path = f'{output_folder}/{year}/{image_id}_properties.json'
        # logging.debug(f'  Bucket TIF:  {tif_path}')
        # logging.debug(f'  Bucket JSON: {json_path}')

        if not overwrite_flag and os.path.isfile(tif_path):
            logging.info('  File already exists and overwrite is false, skipping')
            continue

        input_img = ee.Image(input_img_id).select(input_bands, output_bands)

        if dtype == 'float32':
            output_img = input_img.toFloat()
        else:
            raise ValueError(f'Unsupported output datatype: {dtype}')

        # Save the image to geotiff
        # if overwrite_flag or not os.path.isfile(tif_path):
        logging.debug('  Building output GeoTIFF')
        with rasterio.open(
                temp_path, 'w',
                driver='GTiff',
                tiled=True,
                blockxsize=256,
                blockysize=256,
                # compress='lzw',
                compress='deflate',
                count=len(output_bands),
                dtype=dtype,
                nodata=nodata,
                height=shape[1],
                width=shape[0],
                crs=crs,
                transform=crs_transform,
        ) as output_ds:
            for i, band_name in enumerate(output_bands):
                output_ds.set_band_description(i + 1, band_name)
                output_ds.write(np.full(shape, nodata, dtype=dtype), i + 1)

        logging.debug('  Writing arrays')
        for band_index, band_name in enumerate(output_bands):
            logging.info(f'  Band: {band_name} ({band_index})')
            output_xr = xarray.open_dataset(
                output_img.select([band_name]),
                engine='ee',
                crs=crs,
                crs_transform=crs_transform,
                shape_2d=shape,
                executor_kwargs={'max_workers': workers}
            )
            try:
                output_array = output_xr[band_name].values[0, :, :]
            except Exception as e:
                logging.info('  Error reading array data, skipping')
                os.remove(temp_path)
                break

            with rasterio.open(temp_path, 'r+') as output_ds:
                output_ds.write(output_array, band_index + 1)

            del output_array

        # Copy the image to a COG format
        logging.debug(f'  Converting to COG')
        with rasterio.open(temp_path, 'r') as src_ds:
            data = src_ds.read()
            profile = src_ds.profile.copy()
            with rasterio.open(tif_path, 'w', **profile) as dst_ds:
                dst_ds.descriptions = output_bands
                dst_ds.write(data)

        # Remove the temporary file
        if cleanup:
            os.remove(temp_path)

        if export_properties_json:
            logging.debug(f'  Saving properties JSON')
            # # Remove unneeded properties
            # for k in ['system:footprint']:
            #     if k in scene_info['properties'].keys():
            #         del scene_info[k]
            with open(json_path, 'w') as json_f:
                json.dump(image_info['properties'], json_f, indent=4, sort_keys=True)


def arg_parse():
    """"""
    parser = argparse.ArgumentParser(
        description='Download OpenET reference ET assets to COG',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--timestep', choices=['daily', 'monthly'], metavar='TIME', default='monthly',
        help=f'Timestep (choices:{", ".join(TIMESTEPS)})')
    parser.add_argument(
        '--region', choices=REGIONS, metavar='REGION', default='conus/gridmet',
        help=f'Region/dataset name (choices:{", ".join(REGIONS)})')
    parser.add_argument(
        '--start', required=True, type=utils.arg_valid_date, metavar='YYYY-MM-DD',
        help='Start date')
    parser.add_argument(
        '--end', required=True, type=utils.arg_valid_date, metavar='YYYY-MM-DD',
        help='End date (exclusive)')
    parser.add_argument(
        '--project', required=True,
        help='Google cloud project ID to use for GEE authentication')
    parser.add_argument(
        '--workspace', metavar='PATH', default=os.path.dirname(os.path.abspath(__file__)),
        help='Set the current working directory')
    parser.add_argument(
        '--key', type=utils.arg_valid_file, metavar='FILE',
        help='Earth Engine service account JSON key file to use for GEE initialization')
    parser.add_argument(
        '--overwrite', default=False, action='store_true',
        help='Force overwrite of existing files')
    parser.add_argument(
        '--reverse', default=False, action='store_true',
        help='Process dates in reverse order')
    parser.add_argument(
        '--workers', default=10, type=int,
        help='Number of workers to use in the xarray call')
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
        start_dt=args.start,
        end_dt=args.end,
        project_id=args.project,
        workspace=args.workspace,
        overwrite_flag=args.overwrite,
        reverse_flag=args.reverse,
        gee_key_file=args.key,
        workers=args.workers,
    )
