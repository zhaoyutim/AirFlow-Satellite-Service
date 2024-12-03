"""
swinunetr3d_dags.py

This file defines an Airflow Directed Acyclic Graph (DAG) for performing inference using the SWINUNETR3D model on VIIRS data. 
It orchestrates the following tasks:
1. Downloading satellite data.
2. Processing the downloaded data into mosaic patches.
3. Uploading the inferred results to a specified asset location.

Each task is executed in a Slurm job environment, allowing for parallel processing with specified resources.
"""
import sys
from pathlib import Path
root_path = str(Path(__file__).resolve().parents[1]) + "/"
sys.path.insert(0,root_path)

from airflow import DAG
import datetime
from utils import config
from slurm.operators import SlurmOperator
from utils.config import slurm_config

dir_json = root_path + 'data/VNPL1'
dir_nc = root_path + 'data/VNPNC'
dir_tif = root_path + 'data/VNPIMGTIF'
dir_subset = root_path + 'data/subset'
# products_id_img = ["VJ102IMG","VJ103IMG"] #['VNP02IMG', 'VNP03IMG']
# products_id_mod= ['VJ102MOD','VJ103MOD'] #['VNP02MOD','VNP03MOD']
products_id_img = ['VNP02IMG', 'VNP03IMG']
products_id_mod= ['VNP02MOD','VNP03MOD']
dn_img = ['D','N','B']
dn_mod = ['D']
collection_id = '5200' #'5200'
model = "swinunetr3d"
checkpoint_path = "/home/z/h/zhao2/TS-SatFire/saved_models/model_swinunetr3d_mode_af_num_heads_3_hidden_size_36_batchsize_4_checkpoint_epoch_80_nc_8_ts_2.pth"

ids = ["CANADA","US","EU", "AUS"]#,"AF","AS","SA","OC"]
rois = [[-170,41,-41,73],[-127,24,-66,50],[-24,35,41,72],[106,-50,180,-5]] #,[-20, -35, 52, 37],[25, 5, 180, 81],[-82, -56, -35, 13],[110, -47, 180, -10]]

start_date = (datetime.datetime.today()-datetime.timedelta(days=2)).strftime('%Y-%m-%d')
end_date = (datetime.datetime.today()-datetime.timedelta(days=0)).strftime('%Y-%m-%d')

mode='af'
interval = 2
schedule_interval = ['0 14 * * *','0 14 * * *','0 14 * * *','0 16 * * *','0 18 * * *','0 20 * * *','0 22 * * *']


for i in range(len(ids)):
    dag = DAG(
        'SWINUNETR3D_AF_'+ids[i],
        default_args=config.default_args,
        schedule_interval=schedule_interval[i],
        description='A DAG for inference',
        catchup=False
    )

    with dag:
        download_task = SlurmOperator(
            task_id="download_task",
            script_args={"--id": ids[i],
                         "--roi": rois[i],
                         "--start_date": start_date,
                         "--end_date": end_date,
                         "--dn_img": ['D','N','B'],
                         "--dn_mod": ['D'],
                         "--products_id_img": products_id_img,
                         "--products_id_mod": products_id_mod},
            script=root_path+"scripts/download_viirs.py",
            conda_path=slurm_config['conda_path'],
            cpus_per_task=4,
            num_gpus=0,
            env=slurm_config['env'],
            log_path=slurm_config['log_path'],
            poke_interval=60, 
            timeout=10800,
        )

        mosaic_patches_task = SlurmOperator(
            task_id="mosaic_patches_task",
            script_args={"--id": ids[i],
                         "--roi": rois[i],
                         "--start_date": start_date,
                         "--end_date": end_date,
                         "--mode":mode,
                         "--interval": interval,
                         "--step": 5-10*64/1488,
                        # "--step": 4.57,
                         "--dir_mosaics": f"{root_path}data/VIIRS/mosaics"
                         },
            script=root_path+"scripts/mosaic_patches.py",
            conda_path=slurm_config['conda_path'],
            cpus_per_task=8,
            num_gpus=0,
            env=slurm_config['env'],
            log_path=slurm_config['log_path'],
            poke_interval=60, 
            timeout=10800,
        )

        upload_inferred_task = SlurmOperator(
            task_id="upload_inferred_task",
            script_args={"--id": ids[i],
                         "--model_name": model,
                         "--start_date": start_date,
                         "--end_date": end_date,
                         "--mode": mode,
                         "--checkpoint_path": checkpoint_path,
                         "--ts_len": interval,
                         "--asset_id": f"projects/ee-eo4wildfire/assets/{model}_{ids[i]}",
                         "--dir_tif": f"{root_path}data/VIIRS/model_outputs/{ids[i]}/{model}/reconstructed"},
            script=root_path+"scripts/infer_upload_viirs.py",
            conda_path=slurm_config['conda_path'],
            cpus_per_task=4,
            mem_per_cpu='10000M',
            num_gpus=2,
            env=slurm_config['env'],
            log_path=slurm_config['log_path'],
            poke_interval=60, 
            timeout=10800,
        )


        download_task >> mosaic_patches_task >> upload_inferred_task