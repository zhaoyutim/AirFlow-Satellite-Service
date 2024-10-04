# AirFlow-Satellite-Service

## Project Overview
This repository, `AirFlow-Satellite-Service`, provides pipelines for performing deep learning inference on satellite imagery using Apache Airflow. These pipelines automate tasks such as downloading, preprocessing, and running inference on satellite data, and exporting results to the cloud.

## Repository Structure
- **`dags/`**: Contains all Airflow DAGs (Directed Acyclic Graphs) defining tasks and their dependencies. Each `.py` file corresponds to a specific satellite data pipeline.
- **`data/`**: Directory for input and output data. Raw satellite imagery is downloaded here, and inference results are stored.
- **`models/`**: Code for the deep learning models used in the pipelines.
- **`scripts/`**: Utility scripts for preprocessing satellite data, running models, and handling output data. These are invoked by the DAG tasks.
- **`slurm/`**: Contains the Slurm operator, temporary logs, and `sbatch` scripts.
- **`environment.yml`**: Lists Python dependencies, including Airflow, satellite data libraries (e.g., `rasterio`), and deep learning frameworks.
- **`README.md`**: Provides a high-level overview of the repository and instructions for usage.

### Example Directory Structure
```plaintext
├── dags/
│   ├── pipeline_a.py
│   └── pipeline_b.py
├── data/
├── models/
├── scripts/
│   ├── preprocess.py
│   └── inference.py
├── slurm/
│   └── operators.py
├── requirements.txt
└── README.md
```

> **Note:** I have kept comments in the scripts to a minimum, as I believe over-commenting can reduce code readability.

## Getting Started

### 1. Set Up Airflow

After installing Airflow, you'll need to configure it:

1. Open `airflow.cfg` located in the `/airflow` directory.
2. Update the `sql_alchemy_conn` variable to:

   ```plaintext
   postgresql+psycopg2://airflow_user:airflow_pass@localhost/airflow_db
   ```
3. Set `executor = LocalExecutor` in the config file.
4. Update the `dags_folder` variable to point to the `AirFlow-Satellite-Service/dags` folder.

### 2. Database Setup

Ensure PostgreSQL is installed in your Conda environment. Follow these steps to create and configure the database:

1. Initialize a local database by running:
   ```bash
   initdb -D local_db
   ```
2. Start the PostgreSQL server:
   ```bash
    pg_ctl -D local_db -l logfile start
   ```
3. Create a non-superuser:
   ```bash
    pg_ctl -D local_db -l logfile start
   ```
4. Create the Airflow database owned by the new user:
   ```bash
    createdb --owner=mynonsuperuser airflow_db
   ```

### 3. Set Up Airflow

To start Airflow for the first time, run:
   ```bash
    airflow standalone
   ```
This will initialize the database, create a user, and start all Airflow components. For background operation, start the services with:
   ```bash
    airflow webserver -p 8080 -D
    airflow scheduler -D
   ```
To stop these services, run:
   ```bash
    kill -9 `ps aux | grep airflow | awk '{print $2}'`
   ```
### 4. Initializing Fire Data Pipelines
To initialize the active fire and progression DAGs:

1. Download historical data from FIRMS by visiting [FIRMS NASA Download](https://firms.modaps.eosdis.nasa.gov/download/).
2. Request shapefiles for MODIS and VIIRS (NOAA-21) covering the entire current year.
3. Once downloaded, place the files in the following directories:
   - `data/MODIS/af`
   - `data/VIIRS/af`

   Rename each file to `data` so that each directory contains five files with different extensions, all named `data`.
