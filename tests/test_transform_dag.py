import sys
import os
import glob
import pytest
import pandas as pd
import importlib.util
from unittest.mock import MagicMock

# ==========================================
# 0. CONFIGURATION
# ==========================================
LOCAL_DATA_FOLDER = "C:/Users/valen/paralex-project/airflow/paralex-languages"

# ==========================================
# 1. MOCK AIRFLOW 
# ==========================================
m = MagicMock()
sys.modules["airflow"] = m
sys.modules["airflow.operators"] = m
sys.modules["airflow.providers.standard.operators.python"] = m
sys.modules["airflow.decorators"] = m
sys.modules["airflow.providers.postgres.hooks.postgres"] = m

# ==========================================
# 2. LOAD DAG SCRIPT
# ==========================================
current_test_dir = os.path.dirname(os.path.abspath(__file__))
dag_file_path = os.path.abspath(os.path.join(current_test_dir, "..", "airflow", "dags", "transform_paralex_data.py"))

if not os.path.exists(dag_file_path):
    raise FileNotFoundError(f"Could not find DAG file at: {dag_file_path}")

spec = importlib.util.spec_from_file_location("dag_script", dag_file_path)
dag_script = importlib.util.module_from_spec(spec)
sys.modules["dag_script"] = dag_script
spec.loader.exec_module(dag_script)

# ==========================================
# 3. VERIFICATION LOGIC
# ==========================================
def verify_language_integrity(language, base_folder):
    lang_path = os.path.join(base_folder, language)
    # Print header
    print(f"\n[{language.upper()}] Checking integrity in: {lang_path}")

    if not os.path.exists(lang_path):
        pytest.fail(f"[{language}] Folder not found.")

    # A. Find Feature File
    feat_files = glob.glob(os.path.join(lang_path, "**", "*feature*.csv"), recursive=True)
    if not feat_files:
        pytest.fail(f"[{language}] No feature dictionary found.")
    feature_path = feat_files[0]

    # B. Find Processed File
    proc_files = glob.glob(os.path.join(lang_path, "**", "*_processed.csv"), recursive=True)
    if not proc_files:
        pytest.fail(f"[{language}] No processed file found.")
    processed_path = max(proc_files, key=os.path.getsize)

    # C. Load Data
    try:
        df_feats = pd.read_csv(feature_path, sep=None, engine='python')
        df_proc = pd.read_csv(processed_path, on_bad_lines='skip')
    except Exception as e:
        pytest.fail(f"Error reading CSVs: {e}")

    # Clean Dictionary keys
    df_feats.columns = [str(c).strip().lower() for c in df_feats.columns]

    # D. Build Map
    lookup_map = {}
    for _, row in df_feats.iterrows():
        key = str(row['value_id']).strip()
        category = str(row['feature']).strip() # Preserve Case
        val = row['label'] if 'label' in row and pd.notna(row['label']) else key
        lookup_map[key] = (category, val)

    # E. Sample
    sample_size = 10
    df_sample = df_proc.sample(n=sample_size, random_state=42) if len(df_proc) > sample_size else df_proc
    print(f"   Files loaded. Verifying random sample of {len(df_sample)} rows.")

    # F. Validate
    # Enumerate allows us to know if it's the first row (i==0)
    for i, (idx, row) in enumerate(df_sample.iterrows()):
        cell_raw = str(row['cell'])
        parts = cell_raw.replace(" ", ".").split(".")
        
        # VISUAL CHECK: Print details ONLY for the first row of the sample
        if i == 0:
            print(f"   ---------------------------------------------------------------")
            print(f"   [VISUAL CHECK] Sample Row ID: {idx} | Cell: '{cell_raw}'")

        for part in parts:
            part = part.strip()
            if part in lookup_map:
                target_col, expected_val = lookup_map[part]
                
                # Check Column Existence
                if target_col not in row:
                    pytest.fail(f"[{language}] Missing column '{target_col}'")

                # Check Value Match
                actual_val = row[target_col]
                if pd.isna(actual_val): actual_val = ""
                
                # PRINT COMPARISON (Only for first row)
                if i == 0:
                    print(f"     > Found Code '{part}'")
                    print(f"       Expected: Column '{target_col}' == '{expected_val}'")
                    print(f"       Actual:   Column '{target_col}' == '{actual_val}' ... OK")

                if str(actual_val).strip() != str(expected_val).strip():
                    pytest.fail(
                        f"[{language}] Mismatch Row {idx}.\n"
                        f"   Cell: '{cell_raw}' -> Code: '{part}'\n"
                        f"   Expected ({target_col}): '{expected_val}'\n"
                        f"   Found: '{actual_val}'"
                    )
        
        if i == 0:
            print(f"   ---------------------------------------------------------------")

# ==========================================
# 4. RUN PARAMETRIZED TEST
# ==========================================
@pytest.mark.parametrize("language", dag_script.ACTIVE_LANGUAGES)
def test_language_integrity(language):
    if not os.path.exists(LOCAL_DATA_FOLDER):
        pytest.fail(f"Local path not found: {LOCAL_DATA_FOLDER}")

    verify_language_integrity(language, LOCAL_DATA_FOLDER)