from airflow import DAG
# We use BashOperator because paralex is a CLI tool
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import os

# Define the base path where data should be saved
TARGET_BASE_FOLDER = "/opt/airflow/paralex-languages"

LANG_IDS = {
    "french": 14069226,
    "italian": 14442347,
    "portuguese": 14070141,
    "spanish": 14202916,
    "romanian": 14216644,
}

def create_target_folder():
    """Creates the main directory if it doesn't exist."""
    os.makedirs(TARGET_BASE_FOLDER, exist_ok=True)

with DAG(
    dag_id="download_paralex_languages",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    create_folder = PythonOperator(
        task_id="create_folder",
        python_callable=create_target_folder
    )

    download_tasks = []
    
    for lang, zenodo_id in LANG_IDS.items():
        # logic: create a subfolder for the language, cd into it, and run the command
        # paralex get <ID> downloads the file to the current working directory
        
        bash_cmd = f"""
        mkdir -p {TARGET_BASE_FOLDER}/{lang} && \
        cd {TARGET_BASE_FOLDER}/{lang} && \
        paralex get {zenodo_id}
        """

        task = BashOperator(
            task_id=f"download_{lang}",
            bash_command=bash_cmd
        )
        
        # Add to list for visual layout
        download_tasks.append(task)

    # Set dependencies
    create_folder >> download_tasks