import os
import glob
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# ===========================================
#               CONFIGURATION
# ===========================================
SOURCE_BASE_FOLDER = "/opt/airflow/paralex-languages"

# All languages are now active
ACTIVE_LANGUAGES = ["french", "italian", "portuguese", "spanish", "romanian"]
ALL_LANGUAGES = ["french", "italian", "portuguese", "spanish", "romanian"]

# ===========================================
#          LOGIC: CELL PARSER
# ===========================================
def parse_cell_with_dict(cell_value, lookup_dict):
    """
    Generic Parser.
    Splits cell string by delimiters ('.' or space) and looks up parts in the dictionary.
    
    Examples:
      - Portuguese: 'prs.ind.1sg' -> ['prs', 'ind', '1sg']
      - French:     'ind.prs.1.sg' -> ['ind', 'prs', '1', 'sg']
    
    Returns a Series where index=Feature (e.g. 'Tense') and value=Label (e.g. 'present').
    """
    if pd.isna(cell_value):
        return pd.Series(dtype='object')

    # Normalize separators: replace spaces with dots, then split
    parts = str(cell_value).replace(" ", ".").split('.')
    
    result = {}
    for part in parts:
        part = part.strip()
        # Look up key in dictionary
        if part in lookup_dict:
            category, value = lookup_dict[part]
            # If the category (e.g., 'Tense') already exists, we might overwrite or ignore.
            # Usually these parts represent distinct features.
            result[category] = value
            
    return pd.Series(result)

# ===========================================
#      MAIN TRANSFORMATION TASK
# ===========================================
def transform_language_data(language, **kwargs):
    
    # 1. CHECK IF LANGUAGE IS ENABLED
    if language not in ACTIVE_LANGUAGES:
        print(f"Skipping {language} (Not in ACTIVE_LANGUAGES list)")
        return

    # 2. DEFINE SEARCH SCOPE
    language_path = os.path.join(SOURCE_BASE_FOLDER, language)
    print(f"--- Processing {language.upper()} in {language_path} ---")
    
    if not os.path.exists(language_path):
        raise FileNotFoundError(f"Folder not found: {language_path}")

    # 3. FIND ALL CSVs RECURSIVELY
    # Finds files even if nested deep in subfolders (e.g. french/14069226/...)
    search_pattern = os.path.join(language_path, "**", "*.csv")
    all_csvs = glob.glob(search_pattern, recursive=True)
    
    # Filter out previously processed files
    valid_csvs = [f for f in all_csvs if "_processed.csv" not in f]

    if not valid_csvs:
        print(f"ERROR: No valid CSVs found in {language_path}.")
        raise ValueError("No CSV files found to process.")

    # 4. IDENTIFY SPECIFIC FILES
    feature_file_path = None
    main_file_path = None

    # A) Find Feature Dictionary
    # Look for "feature" in the filename (case insensitive)
    feature_candidates = [f for f in valid_csvs if "feature" in os.path.basename(f).lower()]
    
    if not feature_candidates:
        raise ValueError(f"Could not find a 'feature' CSV file in {language}. Found: {[os.path.basename(f) for f in valid_csvs]}")
    
    # Take the first one found (usually there is only one valid feature file)
    feature_file_path = feature_candidates[0]
    print(f"Using Feature Dictionary: {feature_file_path}")

    # B) Find Main Data File 
    # Logic: Any CSV that is NOT the feature file.
    data_candidates = [f for f in valid_csvs if f != feature_file_path]
    
    if not data_candidates:
         raise ValueError("Found feature file but no data file.")

    # Heuristic: The main data file is usually the largest file in the folder
    main_file_path = max(data_candidates, key=os.path.getsize)
    print(f"Using Main Data File: {main_file_path}")

    # 5. LOAD DICTIONARY
    try:
        print("Loading dictionary...")
        # sep=None allows Python to handle Comma/Tab/Space automatically
        df_feats = pd.read_csv(feature_file_path, sep=None, engine='python')
        
        # Normalize headers to lowercase and strip whitespace
        df_feats.columns = [str(c).strip().lower() for c in df_feats.columns]
        
        # Validation: We need 'value_id' (the code) and 'feature' (the target column)
        if 'value_id' not in df_feats.columns or 'feature' not in df_feats.columns:
            raise ValueError(f"Feature file missing required columns ('value_id', 'feature'). Found: {df_feats.columns.tolist()}")
            
        lookup_map = {}
        for _, row in df_feats.iterrows():
            key = str(row['value_id']).strip()
            category = str(row['feature']).strip()
            
            # Use 'label' if available, otherwise fallback to key
            val = row['label'] if 'label' in row and pd.notna(row['label']) else key
            
            lookup_map[key] = (category, val)
            
        print(f"Dictionary loaded: {len(lookup_map)} entries.")

    except Exception as e:
        print(f"Error loading feature file: {e}")
        raise e

    # 6. PROCESS MAIN DATA
    try:
        print(f"Reading main file...")
        df = pd.read_csv(main_file_path, on_bad_lines='skip')
        df.columns = [c.strip().lower() for c in df.columns]

        if "cell" not in df.columns:
            print(f"WARNING: 'cell' column missing in {os.path.basename(main_file_path)}. Skipping.")
            return

        print(f"Processing {len(df)} rows...")
        
        # Apply Lookup Logic
        # This creates a DataFrame of new columns based on the 'feature' column in the CSV
        features_df = df["cell"].apply(lambda x: parse_cell_with_dict(x, lookup_map))
        
        # Merge back to original data
        df_final = pd.concat([df, features_df], axis=1)
        df_final.fillna("", inplace=True)

        # 7. SAVE
        output_path = main_file_path.replace(".csv", "_processed.csv")
        df_final.to_csv(output_path, index=False)
        
        print(f"SUCCESS! Processed file created at: {output_path}")
        print(f"Added columns: {list(features_df.columns)}")

    except Exception as e:
        print(f"Error processing data: {e}")
        raise e

# ===========================================
#               DAG DEFINITION
# ===========================================
with DAG(
    dag_id="transform_paralex_data",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "linguistics"]
) as dag:

    for lang in ALL_LANGUAGES:
        PythonOperator(
            task_id=f"transform_{lang}",
            python_callable=transform_language_data,
            op_kwargs={"language": lang}
        )

        