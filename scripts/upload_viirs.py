"""
upload_viirs.py

This script facilitates the parallel uploading of VIIRS satellite image files to a specified asset. 
It utilizes multiprocessing to enhance performance by handling multiple uploads concurrently.
"""
import glob
import os
import sys
import argparse
from pathlib import Path
root_path = str(Path(__file__).resolve().parents[1]) + "/"
sys.path.insert(0,root_path)

import datetime
import billiard as multiprocessing
from utils.export import upload_files
import datetime
from utils.config import viirs_config

def upload_in_parallel(id, start_date, end_date, asset_id, dn, type="IMG"):
    """Collects and uploads VIIRS image files for a specified date range and asset ID in parallel."""
    args_list = []
    duration = datetime.datetime.strptime(end_date, '%Y-%m-%d') - datetime.datetime.strptime(start_date, '%Y-%m-%d')
    for k in range(duration.days):
        date = (datetime.datetime.strptime(start_date, '%Y-%m-%d') + datetime.timedelta(k)).strftime('%Y-%m-%d')
        for i in dn:
            files = glob.glob(os.path.join(viirs_config.dir_subset, id, date, i,'VNP'+type+date+'*.tif'))
            for file in files:
                args_list.append([file, asset_id, date])
    print("Found",len(args_list),"files to upload.")
    
    results = []
    with multiprocessing.Pool(processes=1) as pool:
        for args in args_list:
            result = pool.apply_async(upload_files, args)
            results.append(result)
        results = [result.get() for result in results if result is not None]

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--id')
    parser.add_argument('--start_date')
    parser.add_argument('--end_date')
    parser.add_argument('--asset_id')
    parser.add_argument('--dn', type=lambda s: [item for item in s.split(',')])

    args = parser.parse_args()
    upload_in_parallel(args.id, args.start_date, args.end_date, args.asset_id, args.dn, type="IMG")