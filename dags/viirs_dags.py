"""
viirs_dags.py

This file defines a Directed Acyclic Graph (DAG) for processing VIIRS Iband images. 
It includes tasks for downloading images using a SlurmOperator and uploading them 
to Google Earth Engine using a PythonOperator. The DAG is scheduled to run daily 
for specified regions of interest (ROIs) and products.
"""
import sys
from pathlib import Path
root_path = str(Path(__file__).resolve().parents[1]) + "/"
sys.path.insert(0,root_path)

import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.upload_viirs import upload_in_parallel
import datetime
from utils import config
from utils.config import slurm_config
from slurm.operators import SlurmOperator

start_date = (datetime.datetime.today()-datetime.timedelta(days=1)).strftime('%Y-%m-%d')
end_date = (datetime.datetime.today()).strftime('%Y-%m-%d')
dir_json = root_path + 'data/VNPL1'
dir_nc = root_path + 'data/VNPNC'
dir_tif = root_path + 'data/VNPIMGTIF'
dir_subset = root_path + 'data/subset'
product_id = 'IMG'

products_id = ['VNP02'+product_id, 'VNP03'+product_id]
collection_id = '5200'

ids = ["CANADA","US","EU"]
rois = [[-170,41,-41,73],[-127,24,-66,50],[-24,35,41,72]]

dns = ['D', 'N']
schedule_interval = ['0 12 * * *','0 12 * * *','0 12 * * *']

for i in range(len(ids)):
    for dn in dns:
        if dn=='D':
            dn_name = 'Day'
        else:
            dn_name = 'Night'

        dag = DAG(
            f'VIIRS_{dn_name}_{ids[i]}',
            default_args=config.default_args,
            schedule_interval=schedule_interval[i],
            description='A DAG for processing VIIRS Iband images and upload to gee',
            catchup=False
        )

        with dag:
            if dn == 'D':
                download_task = SlurmOperator(
                    task_id="download_task",
                    script_args={"--id": ids[i],
                                "--roi": rois[i],
                                "--start_date": start_date,
                                "--end_date": end_date,
                                "--dn_img": dn,
                                "--products_id_img":products_id,
                                "--products_id_mod": []},
                    script=root_path+"scripts/download_viirs.py",
                    conda_path=slurm_config['conda_path'],
                    env=slurm_config['env'],
                    log_path=slurm_config['log_path'],
                    poke_interval=60, 
                    timeout=7200,
                )
            else:
                download_task = SlurmOperator(
                    task_id="download_task",
                    script_args={"--id": ids[i],
                                "--roi": rois[i],
                                "--start_date": start_date,
                                "--end_date": end_date,
                                "--dn_img": 'B,N',
                                "--products_id_img":products_id,
                                "--products_id_mod": []},
                    script=root_path+"scripts/download_viirs.py",
                    conda_path=slurm_config['conda_path'],
                    env=slurm_config['env'],
                    log_path=slurm_config['log_path'],
                    poke_interval=60, 
                    timeout=7200,
                )

            upload_task = PythonOperator(
                task_id='upload_gee_task',
                python_callable=upload_in_parallel,
                op_kwargs={
                    'id':ids[i],
                    'start_date': (datetime.datetime.today()-datetime.timedelta(days=1)).strftime('%Y-%m-%d'),
                    'end_date': datetime.datetime.today().strftime('%Y-%m-%d'),
                    'asset_id': f'projects/ee-eo4wildfire/assets/VIIRS_Iband_{dn_name}_{ids[i]}',
                    'dn': dn
                },
            )

            download_task >> upload_task
