import os 
import json
import logging
import argparse
from etl_module.Extract import Extract
from etl_module.Transform import Transform
from etl_module.Load import Load

LOG_PATH = "./logs"
CONFIG_PATH = "./configs"

def load_config(config_path: str):
    with open(config_path, "r") as f:
        config = json.load(f)
    logger.info("Read config successful")
    return config

def get_logger(pipeline_name):
    logger = logging.getLogger(f"{pipeline_name}")

    file_handler = logging.FileHandler(f"{LOG_PATH}/{pipeline_name}.log")
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

    logger.addHandler(file_handler)
    logger.setLevel(logging.DEBUG)  # Set desired log level

    return logger


if __name__ == '__main__':
    try:
        # get args
        parser = argparse.ArgumentParser()
        parser.add_argument('--pipeline_name', type=str, required=True)
        args = vars(parser.parse_args())
        
        # set-up logger and config
        pipeline_name = os.path.basename(args["pipeline_name"])
        logger = get_logger(pipeline_name)
        config = load_config(f"{CONFIG_PATH}/{pipeline_name}.json")
        
        # extract
        extractor = Extract(**config["extract"])
        df = extractor.extract_from_db()
        
        print(extractor)
    except Exception as e:
        logger.error(e)