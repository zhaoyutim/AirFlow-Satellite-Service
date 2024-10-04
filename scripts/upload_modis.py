"""
upload_modis.py

This script is designed to upload MODIS satellite image files in parallel to a specified asset. 
It utilizes multiprocessing to enhance performance by handling multiple uploads concurrently.  
"""
import glob
import os
import sys
import argparse
from pathlib import Path
root_path = str(Path(__file__).resolve().parents[1]) + "/"
sys.path.insert(0,root_path)

import billiard as multiprocessing
from utils.export import upload_files
from utils.config import modis_config

def upload_in_parallel(id, date, asset_id):
    """Uploads MODIS TIFF files located in a specified directory for a given ID and date."""
    file_list = glob.glob(os.path.join(modis_config['dir_tif'], id, date, '*.tif'))
    results = []
    with multiprocessing.Pool(processes=8) as pool:
        for file in file_list:
            result = pool.apply_async(upload_files, (file, asset_id, date))
            results.append(result)
        results = [result.get() for result in results if result is not None]

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--id')
    parser.add_argument('--date')
    parser.add_argument('--asset_id')

    args = parser.parse_args()
    upload_in_parallel(args.id, args.date, args.asset_id)