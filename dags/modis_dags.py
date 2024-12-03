"""
modis_dags.py

This file defines a Directed Acyclic Graph (DAG) for processing MODIS images. 
It includes tasks for downloading MODIS data using a SlurmOperator and uploading 
the processed data to Google Earth Engine using a PythonOperator.
"""
import sys
from pathlib import Path
root_path = str(Path(__file__).resolve().parents[1]) + "/"
sys.path.insert(0,root_path)

from easydict import EasyDict as edict
from slurm.operators import SlurmOperator
from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime
from utils import config
from utils.config import slurm_config
from scripts.upload_modis import upload_in_parallel

dir_data = Path(root_path + 'data/MOD09GA')
dir_tif = Path(root_path + 'data/MOD09GATIF')

SOURCE = edict(config.modis_config['MOD09GA'])
products_id = SOURCE.products_id
collection_id = SOURCE.collection_id
date = (datetime.datetime.today()-datetime.timedelta(days=1)).strftime('%Y-%m-%d')
ids = ["NA","EU","AUS"]
hh_lists = [['08', '09', '10', '11', '12', '13', '14'], #NA
            ['17', '18', '19', '20', '21', '22', '23'],
            ['29', '30', '31', '32', '33', '34', '35']] #EU
vv_lists = [['02', '03', '04', '05'],
           ['02', '03', '04', '05'],
           ['10', '11', '12', '13']]
schedule_interval = ['0 12 * * *','0 12 * * *','0 6 * * *']

for i in range(len(ids)):
    dag = DAG(
        f'MODIS_{ids[i]}',
        default_args=config.default_args,
        schedule_interval=schedule_interval[i],
        description='A DAG for processing MODIS images and upload to gee',
        catchup=False
    )
    with dag:
        download_task = SlurmOperator(
            task_id="download_task",
            script_args={"--id": ids[i],
                         "--date": date,
                         "--collection_id": collection_id,
                         "--products_id": products_id,
                         "--hh_list": hh_lists[i],
                         "--vv_list": vv_lists[i],
                         "--format": SOURCE.format,
                         "--bands": SOURCE.BANDS},
            script=root_path+"scripts/download_modis.py",
            conda_path=slurm_config['conda_path'],
            env=slurm_config['env'],
            log_path=slurm_config['log_path'],
            poke_interval=30, 
            timeout=3600,
        )

        upload_task = PythonOperator(
            task_id='upload_gee_task',
            python_callable=upload_in_parallel,
            op_kwargs={
                'id':ids[i],
                'date': date,
                'asset_id': f'projects/ee-eo4wildfire/assets/MODIS_{ids[i]}'
            }
        )

        download_task >> upload_task
