import os
import requests
import zipfile
import yaml
import logging
from datetime import datetime
from io import BytesIO

# --- Load YAML Config ---
def load_config(config_path="configs/bts_ingest.yaml"):
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

# --- Setup Logging ---
def setup_logger(log_file):
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    return logging.getLogger("bts_ingestion")

# --- Download + Extract ---
def download_and_extract(url, output_path):
    response = requests.get(url, stream=True)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch {url}, status={response.status_code}")
    
    with zipfile.ZipFile(BytesIO(response.content)) as z:
        # Assume first CSV inside ZIP
        for file in z.namelist():
            if file.endswith(".csv"):
                z.extract(file, os.path.dirname(output_path))
                os.rename(os.path.join(os.path.dirname(output_path), file), output_path)
                return True
    return False

# --- Main Ingestion ---
def run_ingestion(config_path="configs/bts_ingest.yaml"):
    config = load_config(config_path)
    bts_cfg = config["bts"]
    storage_cfg = config["storage"]
    log_cfg = config["logging"]

    logger = setup_logger(log_cfg["log_file"])

    os.makedirs(storage_cfg["local_raw_dir"], exist_ok=True)

    for year in bts_cfg["years"]:
        for month in bts_cfg["months"]:
            url = bts_cfg["url_template"].format(year=year, month=month)
            filename = storage_cfg["output_filename_template"].format(year=year, month=month)
            output_path = os.path.join(storage_cfg["local_raw_dir"], filename)

            try:
                logger.info(f"Downloading {url}")
                success = download_and_extract(url, output_path)
                if success:
                    logger.info(f"Saved {output_path}")
                else:
                    logger.warning(f"No CSV found in {url}")
            except Exception as e:
                logger.error(f"Failed {url} -> {str(e)}")

if __name__ == "__main__":
    run_ingestion()
