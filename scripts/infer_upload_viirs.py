"""
infer_upload_viirs.py

This script performs inference on VIIRS satellite data, reconstructs images from the inference results, 
and uploads the processed files. It includes methods for running the inference, saving fire locations, 
reconstructing images, and uploading files in parallel.
"""
import glob
import os
os.environ["CUDA_VISIBLE_DEVICES"]="6,7,8,9"
import sys
from pathlib import Path
root_path = str(Path(__file__).resolve().parents[1]) + "/"
sys.path.insert(0,root_path)

import datetime
import datetime
import utils.main_mosaic as main_mosaic
import numpy as np
import rasterio
import models.inferVIIRS as infer
import billiard as multiprocessing
import argparse
from scipy.ndimage import label
from utils.export import upload_files
from utils.config import viirs_config

def inference(id, model_name, start_date, end_date, mode, checkpoint_path, ts_len):
    """Executes the inference process using a specified model and saves the results."""
    print("Starting inference...")
    if mode == 'ba':
        data_path = os.path.join(viirs_config['dir_mosaics'], "batched_patchesBA", id)
    else:
        data_path = os.path.join(viirs_config['dir_mosaics'], "batched_patches", id)
    output_path = os.path.join(root_path, "data/VIIRS/model_outputs", id)
    os.makedirs(data_path, exist_ok=True)
    os.makedirs(output_path, exist_ok=True)
    if mode == 'ba':
        infer.run(model_name,mode,4,3,36,8,ts_len,'ar',str(start_date),str(end_date),output_path,data_path,checkpoint_path)
    else:
        infer.run(model_name,mode,4,3,36,8,ts_len,'v1',str(start_date),str(end_date),output_path,data_path,checkpoint_path)
    print("Inference completed.")

def save_fire_location(image, roi_float, date, diff, save_path, min_cluster_size = 5):
    """Saves the locations of detected fires based on the processed image data."""
    os.makedirs(save_path, exist_ok=True)
    roi = [roi_float[0]+diff, roi_float[1]+diff, roi_float[2]-diff, roi_float[3]-diff]
    structure = np.array([[1, 1, 1],
                          [1, 1, 1],
                          [1, 1, 1]])

    labeled_array, num_features = label(image, structure=structure)
    cluster_sizes = np.bincount(labeled_array.ravel())
    cluster_sizes = cluster_sizes[1:]
    valid_clusters = np.where(cluster_sizes >= min_cluster_size)[0] + 1
    
    print("save_fire_location",roi_float)
    print("img shape",image.shape)
    print("sizes",cluster_sizes)
    print("valid", valid_clusters)
    print("lb shape", labeled_array.shape)
    
    coordinates = []
    for cluster in valid_clusters:
        cluster_mask = (labeled_array == cluster)
        rows = np.any(cluster_mask, axis=1)
        cols = np.any(cluster_mask, axis=0)
        row_min, row_max = np.where(rows)[0][[0, -1]]
        col_min, col_max = np.where(cols)[0][[0, -1]]
        
        approx_coords = [
            roi[0] + np.abs(roi[2] - roi[0]) * (col_min + 1) / image.shape[1] - 0.15,
            roi[3] - np.abs(roi[3] - roi[1]) * (row_min + 1) / image.shape[0] - 0.15,
            roi[0] + np.abs(roi[2] - roi[0]) * (col_max + 1) / image.shape[1] + 0.15,
            roi[3] - np.abs(roi[3] - roi[1]) * (row_max + 1) / image.shape[0] + 0.15
        ]
        
        if (approx_coords[0] < roi_float[0] or approx_coords[1] < roi_float[1] or
            approx_coords[2] > roi_float[2] or approx_coords[3] > roi_float[3]):
            raise ValueError(f"Approximate coordinates {approx_coords} are outside the ROI {roi_float}.")
        
        coordinates.append(approx_coords)
    
    filtered_boxes = []
    for box1 in coordinates:
        contained = False
        for box2 in coordinates:
            if box1 != box2 and is_contained(box1, box2):
                contained = True
                break
        if not contained:
            filtered_boxes.append(box1)
    print("Found non-unique areas: ",len(coordinates)-len(filtered_boxes))

    with open(os.path.join(save_path, np.datetime_as_string(date, unit='D')+".txt"), 'a') as file:
        for bbox in filtered_boxes:
            file.write(f'{",".join(map(str,bbox))}\n')

def is_contained(rect1, rect2):
    """Checks if one rectangle is contained within another."""
    x1_min, y1_min, x1_max, y1_max = rect1
    x2_min, y2_min, x2_max, y2_max = rect2
    
    return (x1_min >= x2_min and x1_max <= x2_max and
            y1_min >= y2_min and y1_max <= y2_max)

def reconstruct_image(id, model_name, mode, start_date, end_date):
    """Reconstructs images from the inference results and saves them."""
    if mode == 'ba':
        data_path = os.path.join(root_path, "data/VIIRS/model_outputs", id, model_name+'BA', "raw")
        save_path = os.path.join(root_path, "data/VIIRS/model_outputs", id, model_name+'BA', "reconstructed")
    else:
        data_path = os.path.join(root_path, "data/VIIRS/model_outputs", id, model_name, "raw")
        save_path = os.path.join(root_path, "data/VIIRS/model_outputs", id, model_name, "reconstructed")
    os.makedirs(save_path, exist_ok=True)

    print('Reconstructing...')
    dates_list = list(np.arange(np.datetime64(start_date), np.datetime64(end_date)))
    ts_len = (datetime.datetime.strptime(end_date, '%Y-%m-%d') - datetime.datetime.strptime(start_date, '%Y-%m-%d')).days
    paths = glob.glob(data_path+'/'+ start_date + '-' + end_date +'*.npy')
    print('Found ',len(paths),' files.')

    pattern = os.path.join(root_path,"LowResSatellitesService/data/S2/locations",id, '*.txt')
    
    for file in glob.glob(pattern):
        file_name = os.path.basename(file)
        file_date_str = file_name.replace('.txt', '')  # Remove .txt extension        
        file_date = datetime.strptime(file_date_str, "%Y-%m-%d")
        
        if start_date <= file_date <= end_date:
            os.remove(file)
            print(f"{file} deleted successfully.")

    for path in paths:
        reconstructed_image = np.zeros(shape=(ts_len,1488-128,1488-128))
        roi = path.split('/')[-1].split('_')[1:]
        roi[-1] = roi[-1][:-4]
        roi_float = list(np.float_(roi))
        roi_string = '_'.join(list(np.round(np.float_(roi),2).astype(str)))
        data = np.load(path)[:,:,64:128+64,64:128+64]
        data = np.reshape(data, (11, 11, ts_len, 128, 128))
        for i in range((1488-128)//128):
            for j in range((1488-128)//128):
                reconstructed_image[:,128*i:128*(i+1),128*j:128*(j+1)] += data[i,j,:,:,:]
        for j in range((1488-128)//128):
            reconstructed_image[:,1488-2*128:,128*j:128*(j+1)]+=data[10,j,:,:,:]
        for i in range((1488-128)//128):
            reconstructed_image[:,128*i:128*(i+1),1488-2*128:]+=data[i,10,:,:,:]
        reconstructed_image[:,1488-2*128:,1488-2*128:]+=data[10,10,:,:,:]
        reconstructed_image = reconstructed_image>0

        for i in range(len(dates_list)):
            print("Saving .tif with", np.sum(reconstructed_image[i]==1), "active fire pixels detected.")
            image = np.expand_dims(reconstructed_image[i,:,:],axis=0)
            
            diff = 5*64/1488
            save_fire_location(reconstructed_image[i,:,:], roi_float, dates_list[i], diff, root_path+"data/S2/locations/"+id)
            print("reconstruct_image",roi_float)
            transform = rasterio.transform.from_bounds(roi_float[0]+diff, 
                                           roi_float[1]+diff, 
                                           roi_float[2]-diff,
                                           roi_float[3]-diff,
                                           width=image.shape[1], 
                                           height=image.shape[2])
            metadata = {
                'driver': 'GTiff', 
                'dtype': 'float32', 
                'nodata': 0.0, 
                'width': image.shape[1], 
                'height': image.shape[2],
                'crs': rasterio.crs.CRS.from_epsg(4326),
                "count": 1,
                "transform": transform
            }

            print("Reconstructed image to shape ", image.shape)

            main_mosaic.write_tiff(os.path.join(save_path, str(dates_list[i]) +"_"+roi_string+".tif"),image,metadata)

def upload_in_parallel(start_date, end_date, asset_id, dir_tif):
    """Uploads the reconstructed images to a specified asset in parallel using multiprocessing."""
    args_list = []
    dates_list = list(np.arange(np.datetime64(start_date), np.datetime64(end_date)))
    for date in dates_list:
        files = glob.glob(os.path.join(dir_tif, str(date)+'*.tif'))
        for file in files:
            args_list.append([file, asset_id, date])
    
    print("Found",len(args_list),"files to upload.")
    
    results = []
    with multiprocessing.Pool(processes=4) as pool:
        for args in args_list:
            result = pool.apply_async(upload_files, args)
            results.append(result)
        results = [result.get() for result in results if result is not None]

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--id')
    parser.add_argument('--model_name')
    parser.add_argument('--start_date')
    parser.add_argument('--end_date')
    parser.add_argument('--mode')
    parser.add_argument('--checkpoint_path')
    parser.add_argument('--ts_len', type=int)
    parser.add_argument('--asset_id')
    parser.add_argument('--dir_tif')

    args = parser.parse_args()
    import psutil
    process = psutil.Process()
    print("Memory used:", psutil.Process(os.getpid()).memory_info().rss / 1024 ** 2)
    inference(args.id, args.model_name, args.start_date, args.end_date, args.mode, args.checkpoint_path, args.ts_len)
    print("Memory used:", psutil.Process(os.getpid()).memory_info().rss / 1024 ** 2)
    reconstruct_image(args.id, args.model_name, args.mode, args.start_date, args.end_date)
    print("Memory used:", psutil.Process(os.getpid()).memory_info().rss / 1024 ** 2)
    upload_in_parallel(args.start_date, args.end_date, args.asset_id, args.dir_tif)
    print("Memory used:", psutil.Process(os.getpid()).memory_info().rss / 1024 ** 2)