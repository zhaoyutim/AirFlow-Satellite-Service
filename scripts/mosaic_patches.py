"""
mosaic_patches.py

This script processes satellite imagery by creating mosaics and patching regions of interest (ROIs) from the images. 
It utilizes multiprocessing to handle large datasets efficiently.
"""
import sys
from pathlib import Path
root_path = str(Path(__file__).resolve().parents[1]) + "/"
sys.path.insert(0,root_path)
import os
import billiard as multiprocessing
import datetime
import utils.main_mosaic as main_mosaic
import argparse
import numpy as np
import glob
import subprocess

def batch_patches(paths, save_path):
    """Combines multiple patches into a single batched array and saves it."""
    path = paths[0]
    tif, _ = main_mosaic.read_tiff(path)
    image = np.array(tif)
    patched_image = patch_image(image)

    for path in paths[1:]:
        tif, _ = main_mosaic.read_tiff(path)
        image = np.array(tif)
        patched_image = np.concatenate((patched_image, patch_image(image)),axis=2)
    np.save(save_path, patched_image)
    print("Saved batched pathches", save_path)

def patch_image(img):
    """Extracts and patches sections of an image."""
    patched_image = np.zeros(shape=(11,11,8,256,256))
    for i in range(1488//128-1):
        for j in range(1488//128-1):
            patched_image[i,j,:,:,:] = img[:,128*i:128*(i+2),128*j:128*(j+2)]
    for j in range(1488//128-1):
        patched_image[10,j,:,:,:] = img[:,1488-256:,128*j:128*(j+2)]
    for i in range(1488//128-1):
        patched_image[i,10,:,:,:] = img[:,128*i:128*(i+2),1488-256:]
    patched_image[10,10,:,:,:] = img[:,1488-256:,1488-256:]
    return np.expand_dims(flatten(patched_image,3),axis=2)

def patch_region(id,file,roi,date):
    """Crops a specified region from a satellite image and saves it to a designated path."""
    product_id = "IMG" if file else "MOD"
    dn = file.split("/")[-2]
    print("Cropping image ", file)
    output_path = os.path.join(root_path + "data/VIIRS/patched_regions", id, date, dn, str(roi[0]) + '_' + str(roi[1]) + '_' + str(roi[2]) + '_' + str(roi[3]))
    os.makedirs(output_path,exist_ok=True)
    output_path = os.path.join(output_path, file.split('/')[-1])
    if os.path.exists(output_path.replace('VNP'+product_id, 'VNP' +product_id + 'PRO')):
        return
    cmd='gdalwarp '+'-te ' + str(roi[0]) + ' ' + str(roi[1]) + ' ' + str(roi[2]) + ' ' + str(roi[3]) + ' ' + file + ' ' + output_path + " -wo USE_OPENCL=TRUE"
    print(cmd)
    subprocess.call(cmd.split())
    print("Completed crop. Saved file at ", output_path)              

def create_mosaic(id, roi, date, dir_subset):
    """Generates a mosaic from a set of TIFF images and saves it."""
    roi_string = '_'.join(str(x) for x in roi)
    date_path = os.path.join(root_path,'data','VIIRS','mosaics', id, date)
    os.makedirs(date_path,exist_ok=True)
    save_path = os.path.join(date_path,'VNP'+roi_string+'.tif')
    print('Processing: ' + date)
    print('Looking for files at', os.path.join(dir_subset, id, date, 'D', roi_string, 'VNPIMG'+'*.tif'))
    img_day_tiff_files = glob.glob(os.path.join(dir_subset, id, date, 'D', roi_string, 'VNPIMG'+'*.tif'), recursive=True)
    img_night_tiff_files = glob.glob(os.path.join(dir_subset, id, date, 'N', roi_string, 'VNPIMG'+'*.tif'), recursive=True)
    img_night_tiff_files.extend(glob.glob(os.path.join(dir_subset, id, date, 'B', roi_string, 'VNPIMG'+'*.tif'), recursive=True))
    mod_tiff_files = glob.glob(os.path.join(dir_subset, id, date, 'D', roi_string, 'VNPMOD*.tif'), recursive=True)
    tiff_files =[img_day_tiff_files,mod_tiff_files,img_night_tiff_files]
    channel_names=['D','MOD','BN']
    print('Found ', len(img_day_tiff_files)+len(img_night_tiff_files)+len(mod_tiff_files), ' files')
    print('Remove Nan')
    combine_paths = []

    for channel in range(3):
        for file in tiff_files[channel]:
            print("Reading file: ", file)
            array, profile = main_mosaic.read_tiff(file)
            array = np.nan_to_num(array)
            main_mosaic.write_tiff(file, array, profile)
        print('Finish remove Nan')

        if len(tiff_files[channel]) !=0:
            mosaic, mosaic_metadata = main_mosaic.mosaic_geotiffs(tiff_files[channel])
            os.makedirs(os.path.join(root_path+'data/VIIRS/mosaics/channels',id,date,channel_names[channel]),exist_ok=True)
            output_path = os.path.join(root_path+'data/VIIRS/mosaics/channels',id,date, channel_names[channel], 'VNP'+roi_string+'.tif')
            main_mosaic.write_tiff(output_path, mosaic, mosaic_metadata)
            combine_paths.append(output_path)
            print("Created mosaic for ", channel_names[channel])
    
    main_mosaic.combine_tiff(combine_paths, save_path)
    print('Finish Creating mosaic ', save_path)    

def mosaic_wrapper(args):
    """A wrapper function for creating mosaics, used for multiprocessing."""
    return create_mosaic(*args)

def patch_region_wrapper(args):
    """A wrapper function for patching regions, used for multiprocessing."""
    return patch_region(*args)

def patch_image_wrapper(args):
    """A wrapper function for batch patching images, used for multiprocessing."""
    return batch_patches(*args)

def get_patch_images_tasks(id, start_date, end_date, roi, step, interval, dir_mosaics):
    """Generates tasks for patching images over a specified date range and ROI."""
    tasks = []
    duration = datetime.datetime.strptime(end_date, '%Y-%m-%d') - datetime.datetime.strptime(start_date, '%Y-%m-%d')
    x_start, x_stop = np.arange(roi[0],roi[2],step), np.arange(roi[0]+5,roi[2]+5,step)
    y_start, y_stop = np.arange(roi[1],roi[3],step), np.arange(roi[1]+5,roi[3]+5,step)

    for i in range(len(x_start)):
        for j in range(len(y_start)):            
                roi_string = '_'.join(str(x) for x in np.array([x_start[i], y_start[j], x_stop[i], y_stop[j]]))
                batched_tasks = []
                for l in range(duration.days//interval):
                    date = (datetime.datetime.strptime(start_date, '%Y-%m-%d') + datetime.timedelta(l*interval)).strftime('%Y-%m-%d')
                    from_date = date
                    path = os.path.join(dir_mosaics,id,date,'VNP'+roi_string+'.tif')
                    batched_tasks.append(path)
                    for k in range(interval-1):
                        date = (datetime.datetime.strptime(start_date, '%Y-%m-%d') + datetime.timedelta(l*interval+k+1)).strftime('%Y-%m-%d')
                        path = os.path.join(dir_mosaics,id,date, 'VNP'+roi_string+'.tif')
                        batched_tasks.append(path)
                    to_date = (datetime.datetime.strptime(start_date, '%Y-%m-%d') + datetime.timedelta(l*interval+k+1)).strftime('%Y-%m-%d')
                    save_path = os.path.join(root_path, 'data/VIIRS/mosaics/batched_patches',id,from_date+'-'+to_date+'_'+roi_string)
                tasks.append([batched_tasks,save_path])
    return tasks

def get_patch_region_tasks(id, start_date,end_date,roi,step):
    """Generates tasks for patching regions over a specified date range and ROI."""
    tasks=[]
    x_start, x_stop = np.arange(roi[0],roi[2],step), np.arange(roi[0]+5,roi[2]+5,step)
    y_start, y_stop = np.arange(roi[1],roi[3],step), np.arange(roi[1]+5,roi[3]+5,step)
    duration = datetime.datetime.strptime(end_date, '%Y-%m-%d') - datetime.datetime.strptime(start_date, '%Y-%m-%d')

    for i in range(len(x_start)):
        for j in range(len(y_start)):            
            for k in range(duration.days):
                date = (datetime.datetime.strptime(start_date, '%Y-%m-%d') + datetime.timedelta(k)).strftime('%Y-%m-%d')
                files = glob.glob(os.path.join(root_path+"data/VIIRS/subset/",id,date)+"/**/*.tif")
                for file in files:
                    tasks.append((id, file, [x_start[i], y_start[j], x_stop[i], y_stop[j]], date))
    return tasks


def get_mosaic_tasks(id, start_date,end_date,rois,dir):
    """Generates tasks for creating mosaics over a specified date range and multiple ROIs."""
    tasks=[]
    duration = datetime.datetime.strptime(end_date, '%Y-%m-%d') - datetime.datetime.strptime(start_date, '%Y-%m-%d')
    for k in range(duration.days):
        for i in range(rois.shape[0]):
            for j in range(rois.shape[1]):
                date = (datetime.datetime.strptime(start_date, '%Y-%m-%d') + datetime.timedelta(k)).strftime('%Y-%m-%d')
                tasks.append((id, rois[i,j,:], date, dir))
    return tasks


def patch_regions(id, roi, start_date, end_date, step):
    """Manages the multiprocessing of patching regions based on generated tasks."""
    patch_region_tasks = get_patch_region_tasks(id, start_date, end_date, roi, step)
    with multiprocessing.Pool(processes=8) as pool:
        list(pool.imap_unordered(patch_region_wrapper, patch_region_tasks))

def patch_images(id, start_date, end_date, dir_mosaics, interval, roi, step):
    """Manages the multiprocessing of patching images based on generated tasks."""
    tasks = get_patch_images_tasks(id, start_date, end_date, roi, step, interval, dir_mosaics)
    with multiprocessing.Pool(processes=8) as pool:
        list(pool.imap_unordered(patch_image_wrapper, tasks))

def flatten(array,except_last_rows):
    """Flattens an array"""
    return array.reshape(-1, *array.shape[-except_last_rows:])

def to_mosaic(id,start_date,end_date,roi,step):
    """Coordinates the creation of mosaics from specified ROIs over a date range."""
    x_start, x_stop = np.arange(roi[0],roi[2],step), np.arange(roi[0]+5,roi[2]+5,step)
    y_start, y_stop = np.arange(roi[1],roi[3],step), np.arange(roi[1]+5,roi[3]+5,step)
    rois = np.zeros(shape=(x_start.shape[0],y_start.shape[0],4))
    for i in range(len(x_start)):
        for j in range(len(y_start)):            
                rois[i,j,:]=np.array([x_start[i], y_start[j], x_stop[i], y_stop[j]])
    
    mosaic_tasks = get_mosaic_tasks(id, start_date,end_date,rois,root_path+"data/VIIRS/patched_regions")
    with multiprocessing.Pool(processes=2) as pool:
        list(pool.imap_unordered(mosaic_wrapper, mosaic_tasks))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--id')
    parser.add_argument('--roi', type=lambda s: [float(item) for item in s.split(',')])
    parser.add_argument('--start_date')
    parser.add_argument('--end_date')
    parser.add_argument('--interval', type=int)
    parser.add_argument('--step', type=float)
    parser.add_argument('--dir_mosaics')

    args = parser.parse_args()
    
    patch_regions(args.id, args.roi, args.start_date, args.end_date, args.step)
    to_mosaic(args.id, args.start_date, args.end_date, args.roi, args.step)
    patch_images(args.id, args.start_date, args.end_date, args.dir_mosaics, args.interval, args.roi, args.step)
        