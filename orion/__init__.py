import logging
import logging.config
import yaml
from pathlib import Path


# Define project base directory
project_dir = Path(__file__).resolve().parents[1]

# Model config
with open(project_dir / 'model_config.yaml', 'rt') as f:
    config = yaml.safe_load(f.read())
