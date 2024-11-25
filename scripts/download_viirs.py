"""
download_viirs.py

This script facilitates the downloading and processing of VIIRS satellite data. It includes methods for 
downloading files, creating JSON and client tasks, reading and projecting data, cropping images, 
and managing multiprocessing tasks for efficiency.
"""
import os
import sys
import datetime
import psutil
from pathlib import Path
root_path = str(Path(__file__).resolve().parents[1]) + "/"
sys.path.insert(0,root_path)

import billiard as multiprocessing
from utils.laads_client import LaadsClient
import argparse
import glob
from  utils.config import viirs_config
import subprocess
import numpy as np
from pyresample import create_area_def
from satpy import find_files_and_readers
from satpy.scene import Scene


laads_client = LaadsClient()

def download(id, roi, start_date, end_date, dn_img, dn_mod, collection_id, products_id_img, products_id_mod):
    """Downloads VIIRS data files based on specified parameters."""
    print("Downloading files from", start_date,"to",end_date)
    duration = datetime.datetime.strptime(end_date, '%Y-%m-%d') - datetime.datetime.strptime(start_date, '%Y-%m-%d')
    area_of_interest = 'W' + str(roi[0]) + ' ' + 'N' + str(roi[3]) + ' ' + 'E' + str(roi[2]) + ' ' + 'S' + str(roi[1])

    json_tasks,client_tasks = [],[]
    if len(dn_img) != 0:
        json_tasks.extend(get_json_tasks(id, start_date, duration, area_of_interest, products_id_img, dn_img, collection_id))
        client_tasks.extend(get_client_tasks(id, start_date, duration, products_id_img, dn_img, collection_id))

    if len(dn_mod) != 0:
        json_tasks.extend(get_json_tasks(id, start_date, duration, area_of_interest, products_id_mod, dn_mod, collection_id))
        client_tasks.extend(get_client_tasks(id, start_date, duration, products_id_mod, dn_mod, collection_id))

    with multiprocessing.Pool(processes=8) as pool:
        list(pool.imap_unordered(laads_client.json_wrapper, json_tasks))

    with multiprocessing.Pool(processes=8) as pool:
        list(pool.imap_unordered(laads_client.client_wrapper, client_tasks))

def get_json_tasks(target_id, start_date, duration, area_of_interest, products_id, day_night, collection_id):
    """Generates a list of JSON tasks for the specified date range and area of interest."""
    tasks = []
    for k in range(duration.days):
        tasks.append(
            (
                target_id,
                (datetime.datetime.strptime(start_date, '%Y-%m-%d') + datetime.timedelta(k)).strftime(
                    '%Y-%m-%d'),
                area_of_interest, products_id, day_night, viirs_config.dir_json, collection_id
            )
        )
    return tasks

def get_client_tasks(id, start_date, duration, products_id, day_night, collection_id):
    """Creates client tasks for downloading data."""
    tasks = []
    for k in range(duration.days):
        tasks.append(
            (
                id,
                (datetime.datetime.strptime(start_date, '%Y-%m-%d') + datetime.timedelta(k)).strftime(
                    '%Y-%m-%d'),
                products_id, day_night, viirs_config.dir_json, viirs_config.dir_nc, collection_id
            )
        )
    return tasks

def read_and_project(dir_nc, date, product_id, bands, dir_tif, dir_chan):
    """Reads the downloaded data, projects it to a specified area, and saves it as GeoTIFF files."""
    time_captured = dir_nc.split('.')[-1][-4:]
    files = find_files_and_readers(base_dir=dir_nc, reader='viirs_l1b')
    files['viirs_l1b'] = [file for file in files['viirs_l1b'] if product_id in file]
    scn = Scene(filenames=files)
    scn.load(bands)
    
    lon_band = 'm_lon' if product_id == 'MOD' else 'i_lon'
    lat_band = 'm_lat' if product_id == 'MOD' else 'i_lat'

    lon = scn[lon_band].values
    lat = scn[lat_band].values

    area = create_area_def(area_id="area", projection='WGS84', shape=(lat.shape[1], lat.shape[0]), lon=lon, lat=lat)
    new_scn = scn.resample(destination=area)

    for n_chan in range(len(bands[:-2])):
        print("n_chan:", n_chan)
        new_scn.save_dataset(
            writer='geotiff', dtype=np.float32, enhance=False,
            filename='{name}.tif',
            dataset_id=bands[n_chan],
            base_dir=dir_nc,
            BIGTIFF='YES')
        if 'MOD' in dir_nc and 'D' in dir_nc and not os.path.exists(os.path.join(dir_nc, f'M11.tif')):
            raise "the day MOD geotiff is not sucessfullly generated, missing channels"
        elif 'IMG' in dir_nc and 'D' in dir_nc and not os.path.exists(os.path.join(dir_nc, f'I0{n_chan+1}.tif')):
            raise "the day IMG geotiff is not sucessfullly generated, missing channels"
        elif 'IMG' in dir_nc and 'N' in dir_nc and not os.path.exists(os.path.join(dir_nc, f'I0{n_chan+4}.tif')):
            raise "the night IMG geotiff is not sucessfullly generated, missing channels"
    tif_files = glob.glob(dir_chan)
    tif_files.sort()

    tif_files_string = ' '.join(map(str, tif_files))

    cmd = "gdalbuildvrt -srcnodata 0 -vrtnodata 0 -separate " + dir_nc + "/VNP"+ product_id + \
            date + '-' + time_captured + ".vrt " + tif_files_string
    print(cmd)
    subprocess.call(cmd.split())
    cmd = "gdal_translate " + dir_nc + "/VNP"+product_id + date +'-'+ time_captured + ".vrt " + dir_tif
    print(cmd)
    subprocess.call(cmd.split())
    
    for tif_file in tif_files:
        os.remove(tif_file)
    del new_scn
    del scn

def crop_to_roi(roi, file, output_path):
    """Crops the processed images to a specified region of interest (ROI)."""
    print("Cropping image ", file)
    cmd='gdalwarp '+'-te ' + str(roi[0]) + ' ' + str(roi[1]) + ' ' + str(roi[2]) + ' ' + str(roi[3]) + ' ' + file + ' ' + output_path + " -wo USE_OPENCL=TRUE "
    print(cmd)
    subprocess.call(cmd.split())
    print("Completed crop. Saved file at ", output_path)
    
def processing(dir_nc, date, roi, product_id, bands, dir_tif, output_path, dir_chan, skip_project=False):
    """Reads and projects files as well as cropping them."""
    print(f'Processiong Date:{date}: {dir_nc} ')
    if not skip_project:
        read_and_project(dir_nc, date, product_id, bands, dir_tif, dir_chan)
    crop_to_roi(roi, dir_tif, output_path)

def process_wrapper(args):
    """A wrapper function for processing tasks in parallel."""
    return processing(*args)

def project(id, roi, start_date, end_date, dn_img, dn_mod):
    """Coordinates the projection of data over a specified date range and ROI."""
    duration = datetime.datetime.strptime(end_date, '%Y-%m-%d') - datetime.datetime.strptime(start_date, '%Y-%m-%d')
    os.makedirs(os.path.join(viirs_config.dir_subset, id),exist_ok=True)
    tasks = get_projection_tasks(start_date, duration, id, roi, dn_img, 'IMG')
    tasks.extend(get_projection_tasks(start_date, duration, id, roi, dn_mod, 'MOD'))
    with multiprocessing.Pool(processes=1) as pool:
        list(pool.imap_unordered(process_wrapper, tasks))

def get_projection_tasks(start_date, duration, id, roi, day_nights, product_id):
    """Generates tasks for projecting data based on the specified parameters."""
    tasks = []
    for k in range(duration.days):
        for dn in day_nights:
            date = (datetime.datetime.strptime(start_date, '%Y-%m-%d') + datetime.timedelta(k)).strftime('%Y-%m-%d')
            dir_ncs = glob.glob(os.path.join(viirs_config.dir_nc,id,date,dn,'*'))
            for dir_nc in dir_ncs:
                if not os.listdir(dir_nc):
                    print("Time does not exist:",dir_nc)
                    continue
                time_captured = dir_nc.split('.')[-1][-4:]
                os.makedirs(os.path.join(viirs_config.dir_tif, id, date, dn), exist_ok=True)
                dir_tif = os.path.join(viirs_config.dir_tif, id, date, dn, "VNP" + product_id + date +'-'+ time_captured + ".tif")
                if os.path.exists(dir_tif):
                    print("The GEOTIFF for time " + date +'-'+ time_captured + " has been created!")
                    skip_project = True
                else:
                    skip_project = False
                if 'MOD' in product_id:
                    bands = ['M11', 'm_lat', 'm_lon']
                    dir_chan = os.path.join(dir_nc, "M[0-9]*.tif")
                else:
                    dir_chan = os.path.join(dir_nc, "I[0-9]*.tif")
                    if dn == 'D':
                        bands = ['I01', 'I02', 'I03', 'I04', 'I05', 'i_lat', 'i_lon']
                    else: 
                        bands = ['I04', 'I05', 'i_lat', 'i_lon']
                os.makedirs(os.path.join(viirs_config.dir_subset, id, date, dn), exist_ok=True)
                output_path = os.path.join(viirs_config.dir_subset, id, date, dn, "VNP" + product_id + date +'-'+ time_captured + ".tif")
                tasks.append(
                    (
                        dir_nc, date, roi, product_id, bands, dir_tif, output_path, dir_chan, skip_project
                    )
                )
    return tasks

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--id')
    parser.add_argument('--roi', type=lambda s: [float(item) for item in s.split(',')])
    parser.add_argument('--start_date')
    parser.add_argument('--end_date')
    parser.add_argument('--dn_img', type=lambda s: [item for item in s.split(',')], default=[])
    parser.add_argument('--dn_mod', type=lambda s: [item for item in s.split(',')], default=[])
    parser.add_argument('--collection_id', default='5200')
    parser.add_argument('--products_id_img', type=lambda s: [item for item in s.split(',')], default=['VNP02IMG', 'VNP03IMG'])
    parser.add_argument('--products_id_mod', type=lambda s: [item for item in s.split(',')], default=['VNP02MOD','VNP03MOD'])

    args = parser.parse_args()
    print(psutil.cpu_percent())
    print(psutil.virtual_memory())
    download(args.id, args.roi, args.start_date, args.end_date, args.dn_img, args.dn_mod, args.collection_id, args.products_id_img, args.products_id_mod)
    print(psutil.cpu_percent())
    print(psutil.virtual_memory())
    project(args.id, args.roi, args.start_date, args.end_date, args.dn_img, args.dn_mod)
    print(psutil.cpu_percent())
    print(psutil.virtual_memory())