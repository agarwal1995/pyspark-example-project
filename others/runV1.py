import json
from datetime import datetime
import yaml

if __name__ == "__main__":
    try:
        with open("config.json", 'r') as stream:
            config = json.load(stream)
        print(f'Config : {config}')
    except Exception as e:
        raise Exception(f"Config load failed: {e}")

    try:
        with open("config.yml", 'r') as stream:
            config = yaml.safe_load(stream)
        print(f'Config : {config}')
    except Exception as e:
        raise Exception(f"Config load failed: {e}")

