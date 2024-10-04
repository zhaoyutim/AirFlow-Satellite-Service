"""
infer_upload_s2.py

This script performs image inference on satellite images and uploads the results to Google Cloud and 
Google Earth Engine. 
"""
import numpy as np
import subprocess
import glob
import sys
import rasterio
import argparse
import os
from pathlib import Path
root_path = str(Path(__file__).resolve().parents[1]) + "/"
sys.path.insert(0,root_path)
from utils import config
import pyproj

def infer(image_path, output_path):
    """Executes inference on images located at the specified image path and saves the output to the given output path."""
    command = f"python3 {root_path}models/inferS2.py --model /home/a/a/aadelow/s2_bam/checkpoint_epoch_31.pth --input {image_path} --output {output_path}"
    print(command)
    subprocess.call(command.split())

    files = glob.glob(os.path.join(output_path,"*.npy"))
    print("Found files:",files)

    for file in files:
        image = np.load(file)
        roi = list(map(float, file.split("/")[-1][:-4].split("_")[3:]))
        local_crs = file.split("/")[-1].split("_")[1]
        wgs84 = pyproj.CRS('EPSG:4326')

        utm = pyproj.CRS(local_crs)
        project = pyproj.Transformer.from_crs(wgs84, utm, always_xy=True).transform

        min_x, min_y = project(roi[0], roi[1])
        max_x, max_y = project(roi[2], roi[3])


        transform = rasterio.transform.from_bounds(min_x, 
                                        min_y, 
                                        max_x, 
                                        max_y,
                                        width=image.shape[0], 
                                        height=image.shape[1])
        metadata = {
            'driver': 'GTiff', 
            'dtype': 'float32', 
            'nodata': 0.0, 
            'width': image.shape[0], 
            'height': image.shape[1],
            'crs': local_crs,
            "count": 1,
            "transform": transform
        }
        with rasterio.Env():
            with rasterio.open(os.path.join(output_path,file.split("/")[-1][:-4]+".tif"), 'w', **metadata) as dst:
                dst.write(image.astype(rasterio.float32),1)

def upload(asset_id, dir_tif):
    paths = glob.glob(os.path.join(dir_tif, '*.tif'))
    print("Begin uploading files:", paths)

    # Create a pool of workers
    from multiprocessing import Pool

    with Pool(processes=8) as pool:
        list(pool.imap_unordered(upload_file, [(path, asset_id) for path in paths]))

def upload_file(args):
    path, asset_id = args
    upload_to_gcloud(path)
    upload_to_gee(path, asset_id=asset_id)
    return f"Uploaded {path}"  # Return a message for each upload

def upload_to_gcloud(file, gs_path='gs://ai4wildfire/VNPPROJ5/'):
    print('Upload to gcloud')

    file_name = file.split('/')[-1]
    file_name = file_name.replace(".","").replace(":","")[:-3] + '.tif'
    id = 's2'
    gs_path += id + '/' + file_name
    upload_cmd = 'gsutil cp ' + file + ' '+gs_path
    print(upload_cmd)
    os.system(upload_cmd)
    print('finish uploading' + file_name)

def upload_to_gee(file, gs_path='gs://ai4wildfire/VNPPROJ5/', asset_id='projects/proj5-dataset/assets/proj5_dataset/'):
    print('start uploading to gee')
    file_name = file.split('/')[-1]
    file_name = file_name.replace(".","").replace(":","")[:-3]

    id = '0000'
    gs_path += "s2" + '/' + file_name + '.tif'
    print(str(file.split('/')[-2]))
    date = file.split('/')[-1].split("_")[0].split("-")
    time_start = f"{date[0]}-{date[1]}-{date[2]}" + 'T' + '00' + ':' + '00' + ':00'
    cmd = config.ee_path + ' upload image --force --time_start ' + time_start + ' --asset_id='+asset_id + '/' + file_name + ' --pyramiding_policy=sample '+gs_path
    print(cmd)
    subprocess.call(cmd.split())
    print('Uploading in progress for image ' + time_start)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--id', default="US")
    parser.add_argument('--asset_id', default="projects/ee-eo4wildfire/assets/S2UNET")

    args = parser.parse_args()

    image_path = root_path+"data/S2/images/"+args.id+"/"
    output_path = root_path+"data/S2/out/"+args.id+"/"
    infer(image_path, output_path)
    upload(args.asset_id, output_path)
    
    files = glob.glob(os.path.join(output_path, "*"))
    for file in files:
        os.remove(file) 
    files = glob.glob(os.path.join(image_path, "*"))
    for file in files:
        os.remove(file) 
