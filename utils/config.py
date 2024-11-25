"""
config.py

This module contains configuration settings for the AirFlow Satellite Service, including
default arguments for tasks, paths for data storage, and specific configurations for MODIS
and VIIRS satellite data processing.
"""
from datetime import timedelta
from datetime import datetime
from pathlib import Path
from easydict import EasyDict as edict
root_path = str(Path(__file__).resolve().parents[1]) + "/"

start_date = (datetime(2024, 8, 13))
default_args = {
    'owner': 'antonadelow',
    'start_date': start_date,
    'depends_on_past': False,
    'email': ['mail@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '0 */8 * * *'
    }

modis_config = {
    'VNP09GA': {
        "products_id": "VNP09GA",
        "collection_id": '5000',
        "eeImgColName": "VIIRS_NRT",
        "format": '.h5',
        "BANDS": ["M3", "M4", "M5", "M7", "M10", "M11", "QF2"]
    },

    'VNP09_NRT': {
        "products_id": "VNP09_NRT",
        "collection_id": '5000',
        "eeImgColName": "VIIRS_NRT",
        "format": '.hdf',
        "BANDS": [
            "375m Surface Reflectance Band I1",
            "375m Surface Reflectance Band I2",
            "375m Surface Reflectance Band I1"
        ]
    },

    'MOD09GA': {
        "products_id": "MOD09GA",
        "collection_id": '61',
        "eeImgColName": "MODIS_NRT",
        "format": '.hdf',
        "BANDS": ["sur_refl_b01_1", "sur_refl_b02_1", "sur_refl_b07_1"]
    },

    'MOD02HKM': {
        "products_id": "MOD02HKM",
        "collection_id": '61',
        "eeImgColName": "MODIS_NRT",
        "format": '.hdf',
        "BANDS": ["sur_refl_b01_1", "sur_refl_b02_1", "sur_refl_b03_1", "sur_refl_b04_1", "sur_refl_b07_1"]
    },
    'dir_tif': root_path + 'data/MODIS/tif',
    'dir_data': root_path + 'data/MODIS/hdf',
    'dir_af': root_path + 'data/MODIS/af'
}

slurm_config = {'conda_path': '/home/z/h/zhao2/miniforge3-clean/etc/profile.d/conda.sh',
                'env': 'lrss',
                'log_path': root_path + 'slurm'}

auth_token = 'eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOiJFYXJ0aGRhdGEgTG9naW4iLCJzaWciOiJlZGxqd3RwdWJrZXlfb3BzIiwiYWxnIjoiUlMyNTYifQ.eyJ0eXBlIjoiVXNlciIsInVpZCI6InpoYW95dXRpbSIsImV4cCI6MTczMzkyMzE4NiwiaWF0IjoxNzI4NzM5MTg2LCJpc3MiOiJodHRwczovL3Vycy5lYXJ0aGRhdGEubmFzYS5nb3YifQ.MjF_mIm4XgpZq7I39KwtvMsyn274oVZqiH93glmVB8NgwC6Wd23w8Ck4TIAA-jOQX-YBFYFvwEojr0FD_rY1LOKMw3C_5egcO4OGaZHF4pR2Bowpk0Uwn9TWHt_COlywJyMdMkdDvpo8_O9QdRiP9gnKUKHOhYTdidZZUHsfTTrm1KEzMmHNEzF6Jtk_957lUqYZ-fYpNDFSzfCAO0T2yLelxfthREvDmpvztmFhMr1m6Lbh5AOdw2EWHcG272rkbkILLO05WtW3ZsPBTQdvpmSv_KITGAII8AqXxrNqfuFk0UuDWxX98JRrmcR_MCQY8SZIWJjhbET88LvK2ljaXg'
ee_path = '/home/z/h/zhao2/miniforge3-clean/envs/lrss/bin/earthengine'
project_name = 'ee-eo4wildfire'

viirs_config = edict({'dir_tif': root_path + 'data/VIIRS/tif',
                      'dir_nc': root_path + 'data/VIIRS/nc',
                      'dir_subset': root_path + 'data/VIIRS/subset',
                      'dir_json': root_path + 'data/VIIRS/json',
                      'dir_af': root_path + 'data/VIIRS/af',
                      'dir_mosaics': root_path + 'data/VIIRS/mosaics'})