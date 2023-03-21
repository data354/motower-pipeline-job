import json
from pathlib import Path
import pandas as pd

config_file = Path(__file__).parents[3] / "config/configs.json"

if config_file.exists():
    with config_file.open("r",) as f:
        config = json.load(f)
else:
    raise RuntimeError("configs file don't exists")

def get_base_site(date: str)-> pd.DataFrame:
    """
        get base_site ans remove unnecessary columns
        Args:
            date [str]
        Return
            pd.DataFrame
    """
    
    files = (Path(config["data"]) / "base_site").glob("*.xlsx")
    if not len(files):
        raise(" dir base_site is empty ")

    file = [f for f in files if f.is_file()][0]