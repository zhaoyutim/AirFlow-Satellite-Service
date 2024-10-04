"""
s2_ba_dags.py

This file defines a set of Directed Acyclic Graphs (DAGs) for downloading Sentinel-2 (S2) data, 
inferring, and uploading it to Google Earth Engine (GEE) for different regions 
(US, EU, CA). Each DAG consists 
of two tasks: downloading the S2 data and uploading the inferred 
results.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
from pathlib import Path
root_path = str(Path(__file__).resolve().parents[1]) + "/"
sys.path.insert(0,root_path)
from utils import config
from utils.config import slurm_config
from slurm.operators import SlurmOperator

ids = ["US","EU","CA"]

for id in ids:
    dag = DAG(
        f'S2_Burned_Area_{id}',
        default_args=config.default_args,
        schedule_interval='0 11 * * *',
        description=f'A DAG for downloading S2 data in {id}, inferring and uploading to gee.',
        catchup=False
    )
    with dag:        
        download_task = SlurmOperator(
            task_id="download_task",
            script_args={
                "--id": id,
                "--folder_path": f"{root_path}data/S2/locations",
                "--output_path": f"{root_path}data/S2/images"
                        },
            script=root_path+"scripts/download_s2.py",
            conda_path=slurm_config['conda_path'],
            env=slurm_config['env'],
            log_path=slurm_config['log_path'],
            poke_interval=30, 
            timeout=3600,
    )
    
        upload_task = SlurmOperator(
            task_id="upload_task",
            script_args={
                "--id": id
                },
            script=root_path+"scripts/infer_upload_s2.py",
            conda_path=slurm_config['conda_path'],
            env=slurm_config['env'],
            log_path=slurm_config['log_path'],
            poke_interval=30, 
            timeout=3600,
    )
        download_task >> upload_task