"""
ideally design

config might looks like this
{
    Load: {
        "provider" : "aws"
        "destination_type" : "data_lake", # or "data_warehouse",
        "load_destination" : "s3://xxxxxxxxx",
        "path_prefix": "%Y-%m-%d",
        "pipeline_name" : "abc"
        "creds_config" : {}


    },
}

"""
from datetime import datetime
import logging
class Load:
    def __init__(self, config) -> None:
        # Validate required fields
        required_fields = {"provider", "destination_type", "load_destination", "path_prefix", "pipeline_name"}
        if not all(key in config for key in required_fields):
            raise ValueError("Missing required configuration parameters: " + ", ".join(required_fields))

        self.provider = config["provider"]
        self.destination_type = config["destination_type"]
        self.load_destination = config["load_destination"]
        self.path_prefix = config["path_prefix"]
        self.pipeline_name = config["pipeline_name"]
        self.creds_config = config.get("creds_config", {})

        self._get_creds()

    def _get_creds(self):
        # Implement logic to retrieve credentials based on provider
        # ... (e.g., using boto3 for AWS, gcloud for GCP)

        # Update self.creds with the retrieved credentials
        # ...
        pass

    def load(self):
        logger = logging.getLogger("load_pipeline")
        progress_counter = 0

        try:
            # Prepare data source using credentials
            data_source = self._prepare_data_source()

            # Get target path
            target_path = self._get_target_path()

            # Write data to target path
            self._write_data(data_source, target_path)

            logger.info(f"Successfully loaded {progress_counter} records to {target_path}")
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            raise e

    def _prepare_data_source(self):
        # Implement logic to access data source based on provider and credentials
        # ...

        # Return the data source object or iterator
        pass

    def _get_target_path(self):
        return f"{self.load_destination}/{datetime.now().strftime(self.path_prefix)}"

    def _write_data(self, data_source, target_path):
        # Implement logic to write data to the target path based on the data source
        # ...

        # Raise an exception if writing fails
        pass
