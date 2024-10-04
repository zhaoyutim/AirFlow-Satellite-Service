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
    print("Reading tiff of shape",image.shape)
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


if __name__=='__main__':
    # Changed the id
    id = 'New_BC_Alberta_danny'
    if not os.path.exists(os.path.join('data/tif_dataset', id)):
        os.mkdir(os.path.join('data/tif_dataset', id))
    # Changing the date
    start_date = '2023-05-12'
    end_date = '2023-07-01'
    
    duration = datetime.datetime.strptime(end_date, '%Y-%m-%d') - datetime.datetime.strptime(start_date, '%Y-%m-%d')
    for k in range(duration.days):
        date = (datetime.datetime.strptime(start_date, '%Y-%m-%d') + datetime.timedelta(k)).strftime('%Y-%m-%d')
        print('Processing: ' + date)
        output_path = os.path.join('data/tif_dataset', id, 'VNPIMG'+date+'_mosaic.tif')
        tiff_files = glob.glob(os.path.join('data/subset/', id, 'VNPIMG'+date+'*.tif'))
        print('Remove Nan')
        for tiff_file in tiff_files:
            array, profile = read_tiff(tiff_file)
            array = np.nan_to_num(array)
            plt.imshow(array[0,:,:])
            plt.show()
            write_tiff(tiff_file, array, profile)
        print('Finish remove Nan')
        mosaic, mosaic_metadata = mosaic_geotiffs(tiff_files)
        write_tiff(output_path, mosaic, mosaic_metadata)
        print('Finish Creating mosaic')