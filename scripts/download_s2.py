"""
download_s2.py

This script connects to Google Earth Engine to download Sentinel-2 imagery based on specified regions of 
interest (ROIs) and date ranges. It calculates cloud scores and NaN percentages for the images, 
filters them based on these metrics, and saves the processed images in a specified output format. 
The script also supports parallel processing of multiple bounding boxes defined in text files.
"""
import ee
import numpy as np
from PIL import Image
import pyproj
import os
import cv2
import argparse
import glob
from datetime import datetime, timezone

ee.Authenticate()
ee.Initialize()

def calculate_cloud_score_s2(img, roi):
        """Computes cloud score for a given image and ROI."""
        cs_plus = "GOOGLE/CLOUD_SCORE_PLUS/V1/S2_HARMONIZED"
        qa_band = "cs_cdf"
        clear_threshold = 0.6

        img = img.linkCollection(ee.ImageCollection(cs_plus), [qa_band])
        cloud_mask = img.select(qa_band).gte(clear_threshold)
        img_masked = img.updateMask(cloud_mask)

        valid_pixels = img.select('B4').mask()
        reducers = ee.Reducer.sum().combine(
            reducer2=ee.Reducer.count(),
            sharedInputs=True
        )
        stats = ee.Image.cat(cloud_mask, valid_pixels).reduceRegion(
            reducer=reducers,
            geometry=roi,
            scale=10,
            maxPixels=1e8,
            bestEffort=True
        )
        clear_pixels = stats.get(qa_band + "_sum")
        valid_pixel_count = stats.get(qa_band + "_count")
        roi_clear_rate = ee.Number(clear_pixels).divide(valid_pixel_count).multiply(100)
        return img_masked.set("ROI_CLEAR_RATE", roi_clear_rate)

def calculate_nan_percentage(img, roi, band):
    """Filters out images with more than a certain percentage of NaN values."""
    total_pixel_area = ee.Image.pixelArea().reduceRegion(
        reducer=ee.Reducer.sum(),
        geometry=roi,
        scale=10,
        maxPixels=1e8,
        bestEffort=True
    ).get('area')

    valid_pixel_area = ee.Image.pixelArea().mask(img.select(band).mask()).reduceRegion(
        reducer=ee.Reducer.sum(),
        geometry=roi,
        scale=10,
        maxPixels=1e8,
        bestEffort=True
    ).get('area')

    nan_percentage = ee.Number(100).subtract(
        ee.Number(valid_pixel_area).divide(total_pixel_area).multiply(100)
    )
    return img.set('NAN_PERCENTAGE', nan_percentage)    

def calculate_properties(img, roi):
    """Combines cloud score and NaN percentage calculations for an image."""
    img = calculate_cloud_score_s2(img, roi)
    img = calculate_nan_percentage(img, roi, 'B4')
    return img

def get_local_crs_by_query(geom):
    """Retrieves the local coordinate reference system for a given geometry."""
    return ee.ImageCollection("COPERNICUS/S2_HARMONIZED")\
                .filterDate("2020-05-01", "2021-01-01")\
                .filterBounds(geom.centroid(ee.ErrorMargin(20))).first()\
                .select(0).projection().crs().getInfo()

def save_to_png(image_array, output_file):
    """Saves an image array as a PNG file."""
    rgb_normalized = cv2.normalize(np.clip(image_array, 0, 5000), None, 0, 255, cv2.NORM_MINMAX, cv2.CV_8U)
    print("RBG SHAPE:", rgb_normalized.shape)
    img = Image.fromarray(rgb_normalized)
    img.save(output_file)

def image_exists(image_id, path):
    """Checks if an image already exists in the specified path."""
    images = glob.glob(f"{path}/*{image_id}*")
    if len(images)>0:
            return True
    return False

def get_data(start_date, roi, output_path, id):
    """Downloads and processes images based on date and ROI criteria."""
    roi_string = '_'.join(str(round(x,2)) for x in roi)    
    geom = ee.Geometry.Rectangle(roi)

    cloud_filter = ee.Filter.gte("ROI_CLEAR_RATE", 80)
    nan_filter = ee.Filter.lte('NAN_PERCENTAGE', 20)
    
    images = (ee.ImageCollection('COPERNICUS/S2_HARMONIZED')
        .filterBounds(geom)
        .filterDate(start_date, datetime.today().strftime('%Y-%m-%d'))
        .map(lambda img: calculate_properties(img, geom))
        .filter(cloud_filter)
        .filter(nan_filter)
        .sort("system:time_start", False)).getInfo()
    
    features = images['features']
    for image in features:
        image_id = image['id']
        exists = image_exists(image_id, output_path)
        if exists:
            continue
        wgs84 = pyproj.CRS('EPSG:4326')
        crsCode = get_local_crs_by_query(geom)
        utm = pyproj.CRS(crsCode)
        project = pyproj.Transformer.from_crs(wgs84, utm, always_xy=True).transform

        min_x, min_y = project(roi[0], roi[1])
        max_x, max_y = project(roi[2], roi[3])

        width = int((max_x - min_x) / 20)
        height = int((max_y - min_y) / 20)
        print(f"Width: {width}, Height: {height}")

        request = {
            'assetId': image_id,
            'fileFormat': 'NUMPY_NDARRAY',
            'bandIds': ['B12', 'B8', 'B4'],
            'grid': {
                'dimensions': {
                    'width': width,
                    'height': height
                },
                'affineTransform': {
                    'scaleX': 20,
                    'shearX': 0,
                    'translateX': min_x,
                    'shearY': 0,
                    'scaleY': -20,
                    'translateY': max_y
                },
                'crsCode': crsCode,
            },
        }

        image_array = ee.data.getPixels(request)
        im_float32 = np.zeros(image_array.shape + (len(image_array.dtype.names),), dtype=np.float32)
        for i, field in enumerate(image_array.dtype.names):
            im_float32[..., i] = image_array[field].astype(np.float32)
        date = datetime.fromtimestamp(image['properties']['system:time_start'] / 1000.0, tz=timezone.utc).strftime('%Y-%m-%d')
        os.makedirs(os.path.join(output_path, id), exist_ok=True)
        output_file = os.path.join(output_path, id, date + '_' + crsCode + '_' + image_id.split("/")[-1].split("_")[2] + "_" + roi_string + '.npy')
        np.save(output_file, im_float32)
        print(f"Image saved as {output_file}")

    if features:
        return True
    else:
        print(f"No image found for {roi_string} within date range.")
        return False 

def process_bounding_boxes(file_path, output_path, id):
    """Processes bounding boxes defined in a text file."""
    start_date = file_path.split("/")[-1][:-4]
    with open(file_path, 'r') as f:
        lines = f.readlines()

    from concurrent.futures import ThreadPoolExecutor

    def process_line(line):
        roi = list(map(float, line.strip().split(",")))
        print(f"Processing ROI: {roi}")
        image_found = get_data(start_date, roi, output_path, id)
        return line if not image_found else None

    def parallelize_lines(lines, num_workers):
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            results = executor.map(process_line, lines)
            updated_lines = set()
            for result in results:
                if result is not None:
                    updated_lines.add(result)
        return updated_lines

    num_workers = 4  # Set the number of workers
    updated_lines = parallelize_lines(lines, num_workers)

    if updated_lines:
        with open(file_path, 'w') as f:
            f.writelines(updated_lines)
    else:
        print(f"No more lines in {file_path}, removing the file.")
        os.remove(file_path)

def process_all_txt_files(folder_path, output_path, id):
    """Processes all text files in a specified folder."""
    txt_files = glob.glob(os.path.join(folder_path, id, "*.txt"))
    print("DEBUG",len(txt_files))
    for txt_file in txt_files:
        print(f"Processing file: {txt_file}")
        process_bounding_boxes(txt_file, output_path, id)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--id')
    parser.add_argument('--folder_path')
    parser.add_argument('--output_path')

    args = parser.parse_args()
    process_all_txt_files(args.folder_path, args.output_path, args.id)