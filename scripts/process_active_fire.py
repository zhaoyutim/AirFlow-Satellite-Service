"""
process_active_fire.py

This script is designed to download, extract, and upload active fire data from the FIRMS 
(Fire Information for Resource Management System) to Google Earth Engine. 
It handles the downloading of data files, extraction of shapefiles, merging of new and existing data, a
nd uploading the processed data to a specified Google Cloud Storage path.
"""
import os
import subprocess
import sys
from pathlib import Path
root_path = str(Path(__file__).resolve().parents[1]) + "/"
sys.path.insert(0,root_path)

import numpy as np
from pathlib import Path
import zipfile
import glob
import geopandas
import pandas
import os.path
import argparse
import urllib.request as Request
import logging

logger = logging.getLogger(__name__)

def download_af_from_firms(url, save_folder):
    """Downloads a file from the specified URL and saves it to the designated folder."""
    print(url)

    save_name = os.path.split(url)[-1]
    dst = Path(save_folder) / save_name
    save_folder = Path(os.path.split(dst)[0])

    logging.basicConfig(
        format='%(asctime)s %(levelname)s %(message)s',
        level=logging.INFO,
        stream=sys.stdout)

    if os.path.isfile(dst):
        os.system("rm {}".format(dst))
        logging.info("Existed file deleted: {}".format(dst))
    else:
        logging.info("File doesn't exist.")

    if not os.path.exists(save_folder):
        os.mkdir(save_folder)

    def down(_save_path, _url):
        try:
            Request.urlretrieve(_url, _save_path)
            return True
        except:
            print('\nError when retrieving the URL:\n{}'.format(_url))
            return False

    # logging.info("Downloading file.")
    down(dst, url)
    print("------- Download Finished! ---------\n")



def download_and_upload(urls, save_folder, asset_id, gspath):
    """Manages the downloading of multiple URLs, extracts the downloaded zip files, merges the shapefiles, and uploads the final zipped shapefile to Google Cloud Storage and Earth Engine."""
    for url in urls:
        download_af_from_firms(url, save_folder)
        print("Extracting...")
        
        new_file_name = os.path.split(url)[-1][:-4]
        
        zip_path = os.path.join(save_folder,new_file_name+'.zip')
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(save_folder)
        os.remove(zip_path)
        
        new_file_path, old_file_path = os.path.join(save_folder,new_file_name+".shp"), os.path.join(save_folder,"data.shp")
        file_name = "data"
        print("Reading files....")
        new_file = geopandas.read_file(new_file_path,engine='pyogrio', use_arrow=True)
        print(new_file.columns)
        new_file['ACQ_DATE'] = pandas.to_datetime(new_file['ACQ_DATE']).astype(np.int64) // 10**6
        old_file = geopandas.read_file(old_file_path,engine='pyogrio', use_arrow=True)
        print("Merging files...")
        gdf = geopandas.GeoDataFrame(pandas.concat([old_file, new_file]))

        print("Saving files...")

        gdf.to_file(os.path.join(save_folder,file_name+'.shp'),engine='pyogrio')
    
    file_name = "data"
    if os.path.isfile(os.path.join(save_folder,file_name+'.zip')):
        os.remove(os.path.join(save_folder,file_name+'.zip'))

    files_to_zip = glob.glob(os.path.join(save_folder,file_name+'.*'))

    print("Zipping...")
    zip_file_path = os.path.join(save_folder,file_name+'.zip') 
    with zipfile.ZipFile(zip_file_path, 'w') as zipf:
        for file in files_to_zip:
            zipf.write(file, os.path.basename(file))

    print("Uploading...")
    upload_to_bucket = f"gsutil -m cp -r {save_folder}/{file_name}.zip {gspath}/{file_name}.zip"
    ee_upload_table = f"earthengine upload table --force --asset_id={asset_id} {gspath}/{file_name}.zip"

    os.system(upload_to_bucket)

    ee_upload_response = subprocess.getstatusoutput(ee_upload_table)[1]
    task_id = ee_upload_response.split("ID: ")[-1]

    print(f"\n{asset_id}")
    print(f"task id: {task_id}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--urls', type=lambda s: [item for item in s.split(',')])
    parser.add_argument('--save_folder')
    parser.add_argument('--asset_id')
    parser.add_argument('--gspath')

    args = parser.parse_args()
    download_and_upload(args.urls, args.save_folder, args.asset_id, args.gspath)
