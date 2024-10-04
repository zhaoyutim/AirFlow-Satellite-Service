"""
download_modis.py

This script facilitates the downloading of MODIS satellite data files based on specified parameters 
such as date, collection ID, and product ID. It also includes functionality to convert the downloaded 
HDF files into GeoTIFF format for easier use in GIS applications.
"""
import glob
import os
import sys
from pathlib import Path
root_path = str(Path(__file__).resolve().parents[1]) + "/"
sys.path.insert(0,root_path)

import rioxarray as rxr
import datetime
import argparse
from utils import config
from utils.config import modis_config

def download_files(id, date, collection_id, products_id, hh_list=['10', '11'], vv_list =['03']):
    """Downloads MODIS data files from specified URLs based on input parameters."""
    year = date[:4]
    julian_day = datetime.datetime.strptime(date, '%Y-%m-%d').timetuple().tm_yday

    dir_data_id = os.path.join(modis_config['dir_data'],id)

    url_part = f"{collection_id}/{products_id}/{year}/{julian_day}"
    dir_data_id_date = os.path.join(dir_data_id,date)
    os.makedirs(dir_data_id_date, exist_ok=True)
    
    for hh in hh_list:
        for vv in vv_list:
            print(f"\nDate: {date}， h{hh}v{vv}")
            print("-----------------------------------------------------------")

            if products_id in ['VNP09GA_NRT']:
                command = "wget -e --timeout=5 robots=off -m -np -R .html,.tmp -nH --cut-dirs=9 " + \
                    f"\"https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/{url_part}/VNP09GA_NRT.A"+str(year)+f"{julian_day}.h{hh}v{vv}.001.h5\" --header \"Authorization: Bearer {config.auth_token}\" \
                        -P {dir_data_id_date}"

            if products_id in ['MOD09GA']:
                command = "wget -e --timeout=5 robots=off -m -np -R .html,.tmp -nH --cut-dirs=9 " + \
                    f"\"https://nrt4.modaps.eosdis.nasa.gov/api/v2/content/archives/allData/{url_part}/{products_id}.A"+str(year)+f"{julian_day}.h{hh}v{vv}.061.NRT.hdf\" --header \"Authorization: Bearer {config.auth_token}\" \
                        -P {dir_data_id_date}"
            if len(glob.glob(os.path.join(dir_data_id_date, f'{products_id}.A'+str(year)+f'{julian_day}.h{hh}v{vv}.061.NRT.hdf')))!=0:
                print('HDF file exist')
                continue
            print(command)
            print(f"\nsaved to: {dir_data_id_date}")
            os.system(command)

def convert_hdf_to_geotiff(id, date, products_id, format, bands):
    """Converts downloaded HDF files to GeoTIFF format."""
    dir_data_with_id = os.path.join(modis_config['dir_data'],id)
    dir_tif_with_id = os.path.join(modis_config['dir_tif'],id)
    fileList = glob.glob(os.path.join(dir_data_with_id,date,products_id+'*'+format)) 
    print("ATTEMPTING CONVERT HDF TO GEOTIFF", len(fileList))
    for file in fileList:
        filename = file.split('/')[-1][:-4]
        print(filename)
        if len(glob.glob(os.path.join(f"{dir_tif_with_id}/{date}", f"{filename.replace('MOD09GA', 'MOD09GATIF')}.tif"))) != 0:
            print('TIF file exist')
            continue

        modis_bands = rxr.open_rasterio(f"{dir_data_with_id}/{date}/{filename}.hdf",
                                masked=True,
                                variable=bands
                        ).squeeze()

        os.makedirs(os.path.join(dir_tif_with_id, date),exist_ok=True)
        modis_bands.rio.reproject("EPSG:4326").rio.to_raster(f"{dir_tif_with_id}/{date}/{filename.replace('MOD09GA', 'MOD09GATIF')}.tif")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--id')
    parser.add_argument('--date')
    parser.add_argument('--collection_id')
    parser.add_argument('--products_id')
    parser.add_argument('--format')
    parser.add_argument('--hh_list', type=lambda s: [item for item in s.split(',')])
    parser.add_argument('--vv_list', type=lambda s: [item for item in s.split(',')])
    parser.add_argument('--bands', type=lambda s: [item for item in s.split(',')])

    args = parser.parse_args()

    download_files(args.id, args.date, args.collection_id, args.products_id, hh_list=['10', '11'], vv_list =['03'])
    convert_hdf_to_geotiff(args.id, args.date, args.products_id, args.format, args.bands)