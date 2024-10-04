"""
export.py

This module provides functionality for uploading files to Google Cloud Storage (GCS) and Google Earth Engine (GEE).
It includes methods to handle the upload process, ensuring that files are correctly transferred to the specified
cloud storage paths and associated with the appropriate asset IDs and timestamps.
"""
import logging
import os
import subprocess
from pathlib import Path
import utils.config
import ee

root_path = str(Path(__file__).resolve().parents[1]) + "/"

logger = logging.getLogger(__name__)

os.environ["GCLOUD_PROJECT"] = utils.config.project_name

ee.Authenticate()
ee.Initialize(project=utils.config.project_name)

def upload_to_gcloud(file, gs_path):
    """Uploads a specified file to Google Cloud Storage at the given path."""
    file_name = file.split('/')[-1]
    gs_path += "/" + file_name
    upload_cmd = 'gsutil cp ' + file + ' '+gs_path
    print(upload_cmd)
    os.system(upload_cmd)

def upload_to_gee(file, gs_path, asset_id, date, time='00' + ':' + '00' + ':00'):
    """Uploads a specified file to Google Earth Engine with the provided asset ID, date, and optional time."""
    file_name = file.split('/')[-1]
    gs_path += "/" + file_name
    time_start = str(date) + 'T' + time
    cmd = utils.config.ee_path + ' upload image --force --time_start ' + time_start + ' --asset_id='+asset_id + "/" + file_name[:-4].replace(".","_") + ' --pyramiding_policy=sample ' + gs_path
    print(cmd)
    subprocess.call(cmd.split())

def upload_files(file, asset_id, date, time='00' + ':' + '00' + ':00', gs_path='gs://ai4wildfire/VNPPROJ5'):
    """Orchestrates the upload process by calling both upload_to_gcloud and upload_to_gee methods."""
    upload_to_gcloud(file, gs_path)
    upload_to_gee(file, gs_path, asset_id, date, time)