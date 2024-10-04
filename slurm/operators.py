"""
operators.py

This module defines the SlurmOperator class, which is a custom Airflow operator for submitting and monitoring 
jobs on a Slurm workload manager. It handles job submission, logging, and error checking.
"""
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import subprocess
import os
import time
from datetime import timedelta
from airflow import AirflowException
import re

class SlurmOperator(BaseOperator):
    @apply_defaults
    def __init__(self, script, conda_path, env, log_path, script_args=[], mem_per_cpu='4600M', cpus_per_task=8, num_gpus=1, poke_interval=60, timeout=3600, *args, **kwargs):
        """Initializes the SlurmOperator with parameters for job configuration and logging."""
        super(SlurmOperator, self).__init__(*args, **kwargs)
        self.poke_interval = poke_interval
        self.timeout = timeout
        self.log_path = log_path
        os.makedirs(log_path,exist_ok=True)

        self.batch_script_content = [
        "#!/usr/bin/env bash",
        f"#SBATCH --output={log_path}/%J_slurm.out",
        f"#SBATCH --error={log_path}/%J_slurm.err",
        f"#SBATCH --time={timedelta(seconds=timeout)}",
        f"#SBATCH --mem-per-cpu={mem_per_cpu}",
        f"#SBATCH --gres=gpu:{num_gpus}",
        f"#SBATCH --cpus-per-task={cpus_per_task}",
        "",
        "# Redirect all output to .out file, and only errors to .err file",
        "exec 3>&1 4>&2",
        "trap 'exec 2>&4 1>&3' 0 1 2 3",
        f"exec 1>{log_path}/${{SLURM_JOB_ID}}_slurm.out 2>&1",
        "",
        f". {conda_path}",
        f"conda activate {env}",
        f"PYTHONUNBUFFERED=1 python3 {script} {self.unparse_args(script_args)} || {{ echo \"Error occurred\" >&2; exit 1; }}"
        ]
        self.log_tracker = [0,0]

    def unparse_args(self, args):
        """Converts script arguments into a string format suitable for command line execution."""
        args_string = ""
        for key, value in args.items():
                if isinstance(value,list):
                    args_string += f" {key}=\"{",".join(map(str, value))}\""
                else:
                    args_string += f" {key}=\"{str(value)}\""
        return args_string
    
    def submit_slurm_job(self):
        """Creates a batch script and submits it to Slurm."""
        batch_script_file = f'{os.path.dirname(os.path.abspath(__file__))}/{self.dag.dag_display_name}_{self.task_id}.sbatch'
        with open(batch_script_file, 'w') as f:
            for line in self.batch_script_content:
                f.write(line + '\n')

        self.start_time = time.time()
        result = subprocess.run(['sbatch', batch_script_file], capture_output=True, text=True)
        self.job_active = True
        os.remove(batch_script_file)

        if result.returncode != 0:
            self.log.error(f"Failed to submit Slurm job: {result.stderr}")
            raise Exception(f"Slurm job submission failed: {result.stderr}")
        
        self.job_id = result.stdout.strip().split()[-1]
        self.log.info(f"Submitted Slurm job with ID: {self.job_id}")
        self.out_path = f'{self.log_path}/{self.job_id}_slurm.out'
        self.err_path = f'{self.log_path}/{self.job_id}_slurm.err'

    def monitor_slurm_job(self):
        """Monitors the status of the submitted Slurm job and checks for errors in the output files."""
        while self.job_active:
            time.sleep(self.poke_interval)

            # Check job status
            command = f"squeue -h -j {self.job_id} -o '%T'"
            result = subprocess.run(command, shell=True, capture_output=True, text=True)

            if result.returncode != 0:
                self.log.error(f"Failed to check Slurm job status: {result.stderr}")
                raise AirflowException(f"Failed to monitor Slurm job status: {result.stderr}")
            
            self.log_slurm()

            job_state = result.stdout.strip()

            if job_state:
                self.log.info(f"Job ID {self.job_id} is currently in state: {job_state}")
            else:
                self.log.info(f"Job ID {self.job_id} is no longer in the queue (likely completed)")
                self.job_active = False
        
        # Check for errors in the .err file
        err_file = f"{self.err_path}"
        if os.path.exists(err_file) and os.path.getsize(err_file) > 0:
            with open(err_file, 'r') as f:
                error_content = f.read().strip()
            
            # Define the pattern for the memory limit warning
            memory_limit_pattern = r'^slurmstepd-[\w-]+: error: Step \d+\.\d+ hit memory limit at least once during execution\. This may or may not result in some failure\.$'
            
            # Check if the error content only contains the memory limit warning
            if re.match(memory_limit_pattern, error_content, re.MULTILINE):
                self.log.warning(f"SLURM job {self.job_id} hit memory limit, but did not fail. Warning: {error_content}")
            else:
                self.cleanup_logs()
                raise AirflowException(f"SLURM job {self.job_id} failed. Error log content: {error_content}")

        # Check job exit status (keep this as a fallback)
        sacct_cmd = f"sacct -j {self.job_id} --format=State,ExitCode -n -P"
        result = subprocess.run(sacct_cmd, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            state, exit_code = result.stdout.strip().split('|')
            if state == "FAILED" or exit_code != "0:0":
                self.cleanup_logs()
                raise AirflowException(f"SLURM job {self.job_id} failed with exit code {exit_code}")
        else:
            self.log.warning(f"Unable to retrieve job exit status: {result.stderr}")

    def log_slurm(self):
        """Reads and logs the output and error files generated by the Slurm job."""
        if os.path.exists(self.out_path):
            with open(self.out_path, 'r') as f:
                for i, line in enumerate(f):
                    if i >= self.log_tracker[0]:
                        self.log.info(f"Slurm Output: {line.strip()}")
                        self.log_tracker[0] = i+1

        if os.path.exists(self.err_path):
            with open(self.err_path, 'r') as f:
                for i, line in enumerate(f):
                    if i >= self.log_tracker[1]:
                        self.log.error(f"Slurm Error: {line.strip()}")
                        self.log_tracker[1] = i+1

    def cleanup_logs(self):
        """Deletes the output and error log files after job completion."""
        if os.path.exists(self.out_path):
            os.remove(self.out_path)

        if os.path.exists(self.err_path):
            os.remove(self.err_path)

    def execute(self, context):
        """Executes the job submission and monitoring process."""
        self.submit_slurm_job()
        self.monitor_slurm_job()
        self.cleanup_logs()
        return self.job_id
    
    def on_kill(self):
        """Cancels the running Slurm job if the task is killed."""
        if self.job_id:
            self.log.info(f"Cancelling Slurm job with ID: {self.job_id}")
            cancel_command = f"scancel {self.job_id}"
            result = subprocess.run(cancel_command, shell=True, capture_output=True, text=True)
            if result.returncode != 0:
                self.log.error(f"Failed to cancel Slurm job: {result.stderr}")
                raise AirflowException("Failed to cancel Slurm job. Marking task as failed.")
            else:
                self.log.info(f"Slurm job {self.job_id} canceled successfully.")
                raise AirflowException("Task was killed and Slurm job was canceled.")
