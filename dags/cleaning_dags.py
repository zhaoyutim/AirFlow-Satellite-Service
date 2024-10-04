"""
cleaning_dags.py

This file defines an Airflow DAG named 'Clean' that is responsible for removing old files both locally and in Google Earth Engine (GEE). It utilizes the SlurmOperator to execute a cleaning script with specified parameters, including the path to data, the number of days to look back, and a list of asset IDs to clean.
"""
import sys
from pathlib import Path
root_path = str(Path(__file__).resolve().parents[1]) + "/"
sys.path.insert(0,root_path)

from airflow import DAG
from utils import config
from slurm.operators import SlurmOperator
from utils.config import slurm_config

asset_prefix = "projects/ee-eo4wildfire/assets/"

dag = DAG(
        'Clean',
        default_args=config.default_args,
        schedule_interval='0 0 * * *',
        description=f'A DAG for removing old files locally and in GEE.',
        catchup=False
    )
with dag:
    clean_task = SlurmOperator(
        task_id="clean_task",
        script_args={"--path": f"{root_path}data",
                     "--days": 2,
                     "--asset_ids": [asset_prefix+"MODIS_EU", asset_prefix+"MODIS_NA", asset_prefix+"VIIRS_Iband_Day_CANADA", asset_prefix+"VIIRS_Iband_Day_EU", asset_prefix+"VIIRS_Iband_Day_US"]},
        script=root_path+"scripts/clean.py",
        conda_path=slurm_config['conda_path'],
        env=slurm_config['env'],
        log_path=slurm_config['log_path'],
        poke_interval=30, 
        timeout=3600,
    )
