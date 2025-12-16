import os
import glob
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

SOURCE_BASE_FOLDER = "/opt/airflow/paralex-languages"
LANGUAGES = ["french", "italian", "portuguese", "spanish", "romanian"]

def get_file_paths(language_folder):
    """
    Identifies the Raw file and the Processed file.
    Returns a dict: {'raw': path_or_none, 'processed': path_or_none}
    """
    search_pattern = os.path.join(language_folder, "**", "*.csv")
    all_csvs = glob.glob(search_pattern, recursive=True)
    
    processed_file = None
    raw_file = None

    # 1. FIND PROCESSED FILE
    processed_candidates = [f for f in all_csvs if "_processed.csv" in f]
    if processed_candidates:
        processed_file = max(processed_candidates, key=os.path.getsize)

    # 2. FIND RAW FILE
    # Exclude processed files AND feature dictionaries
    raw_candidates = [
        f for f in all_csvs 
        if "_processed.csv" not in f 
        and "feature" not in os.path.basename(f).lower()
    ]
    
    if raw_candidates:
        # Heuristic: The main data file is the largest remaining file
        raw_file = max(raw_candidates, key=os.path.getsize)

    return {"raw": raw_file, "processed": processed_file}

def ingest_dataframe(df, table_name, engine):
    """
    Helper function to clean columns and write to SQL
    """
    # Clean Column Names
    df.columns = [
        c.strip().lower()
        .replace(" ", "_")
        .replace("-", "_")
        .replace(".", "_") 
        for c in df.columns
    ]
    
    print(f"-> Ingesting into table '{table_name}'. Schema: {list(df.columns)}")

    with engine.begin() as connection:
        df.to_sql(
            name=table_name,
            con=connection,
            if_exists="replace", # DROPS old table, CREATES new one
            index=False,
            method="multi",
            chunksize=1000
        )

def ingest_language_tasks(language, **kwargs):
    language_path = os.path.join(SOURCE_BASE_FOLDER, language)
    
    files = get_file_paths(language_path)
    
    # Connect to Postgres (We do this once per language task)
    hook = PostgresHook(postgres_conn_id="postgres_paralex")
    engine = hook.get_sqlalchemy_engine()

    # --- INGEST RAW ---
    if files['raw']:
        print(f"Reading RAW file: {files['raw']}")
        try:
            df_raw = pd.read_csv(files['raw'], on_bad_lines='skip')
            # Table name example: 'french_raw'
            ingest_dataframe(df_raw, f"{language}_raw", engine)
        except Exception as e:
            print(f"Error ingesting raw file: {e}")
    else:
        print(f"WARNING: No RAW file found for {language}")

    # --- INGEST PROCESSED ---
    if files['processed']:
        print(f"Reading PROCESSED file: {files['processed']}")
        try:
            df_proc = pd.read_csv(files['processed'], on_bad_lines='skip')
            # Table name example: 'french_processed' 
            ingest_dataframe(df_proc, f"{language}_processed", engine)
        except Exception as e:
            print(f"Error ingesting processed file: {e}")
    else:
        print(f"WARNING: No PROCESSSED file found for {language}")

with DAG(
    dag_id="ingest_paralex_to_postgres",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "postgres"]
) as dag:

    for lang in LANGUAGES:
        PythonOperator(
            task_id=f"ingest_{lang}",
            python_callable=ingest_language_tasks,
            op_kwargs={"language": lang}
        )