# openet-asset-download-tools

Tools for downloading OpenET assets as cloud optimized geotiffs (COG).  These tools were primarily developed to save backup copies of the OpenET assets, and they have no functionality for clipping, mosaicing, reprojecting, etc.

COG files can be either downloaded directly using the python Xee module (https://github.com/google/Xee) or exported to a Google Cloud Storage bucket

The properties for each image will be saved to a "..._properties.json" file.

### COG Download

#### Monthly ET

```commandline
python month_asset_cog_download.py --project <YOUR_PRODUCT_ID> --model ENSEMBLE --version v2_1 --start 2024-07-01 --end 2024-07-31 --mgrs 11S,11T
```

#### Overpass Scene ET

```commandline
python scene_asset_cog_download.py --project <YOUR_PRODUCT_ID> --model ENSEMBLE --start 2024-07-01 --end 2024-07-01 --wrs2 p043r032,p043r033
```

### Bucket Exports

#### Monthly ET

```commandline
python month_asset_bucket_export.py --project <YOUR_PRODUCT_ID> --bucket <YOUR_BUCKET_NAME> --model ENSEMBLE --version v2_1 --start 2024-07-01 --end 2024-07-31 --mgrs 11S,11T
```

#### Overpass Scene ET

```commandline
python scene_asset_bucket_export.py --project <YOUR_PRODUCT_ID> --bucket <YOUR_BUCKET_NAME> --model ENSEMBLE --start 2024-07-01 --end 2024-07-01 --wrs2 p043r032,p043r033
```
