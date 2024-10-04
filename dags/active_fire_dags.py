"""
active_fire_dags.py

This file defines Directed Acyclic Graphs (DAGs) for processing active fire data 
from VIIRS and MODIS sources. It utilizes the SlurmOperator to execute a script 
that processes the active fire data and uploads it to Google Earth Engine (GEE).
"""
import sys
from pathlib import Path
root_path = str(Path(__file__).resolve().parents[1]) + "/"
sys.path.insert(0,root_path)

from pathlib import Path
from airflow import DAG
from utils import config
from slurm.operators import SlurmOperator
from utils.config import slurm_config, viirs_config, modis_config

sources = ["VIIRS","MODIS"]
cfgs = [viirs_config, modis_config]
urls = [["https://firms.modaps.eosdis.nasa.gov/data/active_fire/noaa-20-viirs-c2/shapes/zips/J1_VIIRS_C2_Global_24h.zip","https://firms.modaps.eosdis.nasa.gov/data/active_fire/noaa-21-viirs-c2/shapes/zips/J2_VIIRS_C2_Global_24h.zip"],
        ["https://firms.modaps.eosdis.nasa.gov/data/active_fire/modis-c6.1/shapes/zips/MODIS_C6_1_Global_24h.zip"]]

for i in range(len(sources)):
    dag = DAG(
        f'{sources[i]}_Active_Fire',
        default_args=config.default_args,
        schedule_interval='0 10 * * *',
        description=f'A DAG for processing {sources[i]} Active Fire and upload to gee',
        catchup=False
    )
    with dag:
        process_active_fire_task = SlurmOperator(
            task_id="process_active_fire_task",
            script_args={"--urls": urls[i],
                         "--save_folder": f"{cfgs[i]['dir_af']}", 
                         "--asset_id": f"projects/ee-eo4wildfire/assets/{sources[i]}_AF_2024", 
                         "--gspath": f"gs://ai4wildfire/{sources[i].lower()}_active_fire_nrt"},
            script=root_path+"scripts/process_active_fire.py",
            conda_path=slurm_config['conda_path'],
            env=slurm_config['env'],
            log_path=slurm_config['log_path'],
            poke_interval=30, 
            timeout=3600,
        )
