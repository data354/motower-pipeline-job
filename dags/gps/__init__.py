from pathlib import Path
import json



config_file = Path(__file__).parents[3] / "config/configs.json"
if config_file.exists():
    with config_file.open("r",) as f:
        CONFIG = json.load(f)
else:
    raise RuntimeError("configs file don't exists")