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
        "BANDS": [
            "sur_refl_b01_1",
            "sur_refl_b02_1",
            "sur_refl_b07_1"
        ]
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

slurm_config = {'conda_path': '/home/a/a/aadelow/miniforge3/etc/profile.d/conda.sh',
                'env': 'lrss',
                'log_path': root_path + 'slurm'}

auth_token = 'eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOiJFYXJ0aGRhdGEgTG9naW4iLCJzaWciOiJlZGxqd3RwdWJrZXlfb3BzIiwiYWxnIjoiUlMyNTYifQ.eyJ0eXBlIjoiVXNlciIsInVpZCI6InpoYW95dXRpbSIsImV4cCI6MTcyODcyMTE2MiwiaWF0IjoxNzIzNTM3MTYyLCJpc3MiOiJFYXJ0aGRhdGEgTG9naW4ifQ.8Bzd3NbiQovxFKUCWMPDfLuI4QKuXLmUOd3geytByQeHwbzQgB0i2N1wDWSGYpQRMjAMl64HPm_iwVE-8SWveRG7_OYdqpnYX_4OdB1VCao0Vcm-2Hlk7senJPdGi6ljtqbnVGoxauobZYzu_KDbCK5FSVHoxuWnQP3yJkI2W8SFWSh3Knfql3pDCXzDjUQOMf1AJ0OPXAGMRzdKxpUBV442GA9awi8wbp-100CU8Q6w3FVkvYkojSFWShcmdWglfLl_tMpQ5u5rMSe9X0GS2g9FpJZfIf0JZAlff8OzeBbme_uC1CKylBEtZU2ok9TP1CypGMSlTO6zoCNDNZg2YA'#'eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOiJFYXJ0aGRhdGEgTG9naW4iLCJzaWciOiJlZGxqd3RwdWJrZXlfb3BzIiwiYWxnIjoiUlMyNTYifQ.eyJ0eXBlIjoiVXNlciIsInVpZCI6InpoYW95dXRpbSIsImV4cCI6MTcyMzI5ODY0OSwiaWF0IjoxNzE4MTE0NjQ5LCJpc3MiOiJFYXJ0aGRhdGEgTG9naW4ifQ.JR19mk5uizk0Ar_82bPHA5BihSEgEm7gjJVUWzztoB3v_X7me3cZesrOdBEOTx6qcsYYhUzQuIGlguh_J0va2n7jFn9xMhZCMbBo3yL-Pu58vKJOTk0DVWfgQDI162LRsGNwbDUiS8CeiMg7N-HScBEel1OZA4b9JVI0-nzbsvvgH9w0nOyWnc9bPIIvSWUKazeZ-9sgG6XhRIGLVrwJt5GzWmeNFNIw8B04vJ_YN9EwPtd_vuKmadtEfvOPbiNTqoOK_b4FoeB4-hRO296vuOFeIS4LgVc2zAwxeVTWySFpA4P2FFbTFv_jReVMIVGEaceWhhMDbbxDW7KDyn2afw'
ee_path = '/home/a/a/aadelow/miniforge3/envs/lrss/bin/earthengine'
project_name = 'ee-eo4wildfire'

viirs_config = edict({'dir_tif': root_path + 'data/VIIRS/tif',
                      'dir_nc': root_path + 'data/VIIRS/nc',
                      'dir_subset': root_path + 'data/VIIRS/subset',
                      'dir_json': root_path + 'data/VIIRS/json',
                      'dir_af': root_path + 'data/VIIRS/af',
                      'dir_mosaics': root_path + 'data/VIIRS/mosaics'})