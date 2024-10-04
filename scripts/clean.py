"""
clean.py

This script provides functionality to delete old directories and files based on date criteria 
from a specified root directory, as well as to clean up assets in Google Earth Engine (GEE) 
that are older than a specified number of days.
"""
import glob
import os
import datetime
import argparse
from shutil import rmtree
import ee

def delete_old_date_dirs(root_dir, days, exclude_dirs=None):
    """Deletes directories and files older than a specified number of days."""
    current_date = datetime.datetime.now().date()
    pattern = os.path.join(root_dir, '**', '????-??-??')
    date_dirs = glob.glob(pattern, recursive=True)
    
    for date_dir in date_dirs:
        dir_name = os.path.basename(date_dir)
        if exclude_dirs and date_dir in exclude_dirs:
            continue
        try:
            dir_date = datetime.datetime.strptime(dir_name, '%Y-%m-%d').date()
            days_diff = (current_date - dir_date).days
            
            if days_diff > days:
                print(f"Deleting directory: {date_dir}")
                rmtree(date_dir)
        
        except ValueError:
            print(f"Error: {dir_name} is not a date directory. Resuming...")
            continue
    
    file_pattern = os.path.join(root_dir, '**', '*????-??-??*')
    date_files = glob.glob(file_pattern, recursive=True)
    
    for date_file in date_files:
        file_name = os.path.basename(date_file)
        if any(exclude_dir in date_file for exclude_dir in exclude_dirs):
            continue
        
        try:
            date_str = ''.join(filter(str.isdigit, file_name))[:8]
            file_date = datetime.datetime.strptime(date_str, '%Y%m%d').date()
            days_diff = (current_date - file_date).days
            
            if days_diff > days:
                print(f"Deleting file: {date_file}")
                os.remove(date_file)
        
        except ValueError:
            print(f"Error: Unable to extract date from {file_name}. Skipping...")
            continue
    


def clean_ee(asset_ids, days=14):
    """Deletes images from GEE asset collections that are older than a specified number of days."""
    ee.Initialize()
    cutoff_date = datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=days)
    cutoff_date_str = cutoff_date.strftime('%Y-%m-%d')

    for asset_id in asset_ids:
        print("Deleting images from asset ", asset_id)
        collection = ee.ImageCollection(asset_id)
        filtered_collection = collection.filterDate('0000-01-01', cutoff_date_str)
        image_ids = filtered_collection.aggregate_array('system:id').getInfo()

        for asset_id in image_ids:
            delete_asset(asset_id)

def delete_asset(asset_id):
    """Deletes a specific asset from GEE."""
    try:
        ee.data.deleteAsset(asset_id)
        print(f"Deleted asset: {asset_id}")
    except Exception as e:
        print(f"Error deleting asset {asset_id}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--path')
    parser.add_argument('--days',type=int)
    parser.add_argument('--asset_ids', type=lambda s: [item for item in s.split(',')])

    args = parser.parse_args()
    exclude_dirs = ["/home/a/a/aadelow/LowResSatellitesService/data/S2"]
    delete_old_date_dirs(args.path, args.days, exclude_dirs)
    clean_ee(args.asset_ids)
