"""
main_mosaic.py

This module provides functionality for processing and merging GeoTIFF images. It includes methods for reading, 
writing, and combining TIFF files, as well as creating a mosaic from multiple GeoTIFF images.
"""
import datetime
import glob
import os
import matplotlib.pyplot as plt
import numpy as np
import rasterio
from rasterio.merge import merge

def mosaic_geotiffs(geotiff_files):
    """Merges multiple GeoTIFF images into a single mosaic using maximum values for overlapping pixels."""
    # Read images and metadata
    src_files = [rasterio.open(file) for file in geotiff_files]
    file_path = geotiff_files[0]
    if '/D/' in file_path and 'MOD' in file_path:
        src_files = list(filter(lambda x: (x.count==1), src_files))  
    elif '/N/' in file_path and 'IMG' in file_path:
        src_files = list(filter(lambda x: (x.count==2), src_files))  
    elif '/D/' in file_path and 'IMG' in file_path:
        src_files = list(filter(lambda x: (x.count==5), src_files))  
    # Merge images using maximum values for overlapping locations
    mosaic, out_transform = merge(src_files, method="max", nodata=0.0, res=0.00336)

    # Copy metadata from the first file
    out_meta = src_files[0].meta.copy()

    # Update metadata with the mosaic dimensions and transform
    out_meta.update({
        "height": mosaic.shape[1],
        "width": mosaic.shape[2],
        "transform": out_transform
    })

    # Close source files
    for src in src_files:
        src.close()

    return mosaic, out_meta

def write_tiff(file_path, arr, profile):
    """Writes a NumPy array to a GeoTIFF file with the specified metadata profile."""
    with rasterio.Env():
        with rasterio.open(file_path, 'w', **profile) as dst:
            dst.write(arr.astype(rasterio.float32))

def read_tiff(file_path):
    """Reads a GeoTIFF file and returns the image array and its metadata."""
    with rasterio.open(file_path) as src:
        image = src.read()
        metadata = src.meta
    # print("Reading tiff of shape",image.shape)
    return image, metadata

def combine_tiff(file_paths,output_path,axis=0):
    """Combines multiple TIFF images along a specified axis and writes the result to a new file."""
    image1, metadata1 = read_tiff(file_paths[0])
    for i in range(len(file_paths)-1):
        image2, metadata2 = read_tiff(file_paths[i+1])
        assert metadata1["width"] == metadata2["width"] and metadata1["height"] == metadata2[
"height"], "Images must have the same dimensions. Image 1:" + str(metadata1["width"]) + "x" + str(metadata1["height"]) + ". Image 2: " + str(metadata2["width"]) + "x" + str(metadata2["height"])
        combined_image = np.concatenate((image1, image2), axis=axis)
        image1 = combined_image
        combined_metadata = metadata1.copy()
        combined_metadata["count"] = combined_image.shape[0]
        print("Creating image of shape ", combined_image.shape)
        write_tiff(output_path, combined_image, combined_metadata)

def stack_mir_on_base_img(base_img_path, save_path):
    base_img, base_metadata = read_tiff(base_img_path)
    date_img, date_metadata = read_tiff(save_path)

    for i in [3, 4, 6, 7]:
        date_img[i, ...] = np.maximum(base_img[i, ...], date_img[i, ...])
    
    base_img = date_img
    print("Update date img and base img at location:", save_path)
    write_tiff(base_img_path, base_img, base_metadata)
    write_tiff(save_path.replace('VNP', 'BA'), date_img, date_metadata)

if __name__=='__main__':
    # Changed the id
    img_day_tiff_files = glob.glob(os.path.join('/home/z/h/zhao2/AirFlow-Satellite-Service/data/VIIRS/patched_regions/EU/2024-11-19/D/-170.0_50.13978494623656_-165.0_55.13978494623656', 'VNPIMG'+'*.tif'), recursive=True)
    mosaic_geotiffs(img_day_tiff_files)