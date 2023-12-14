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
    def __init__(self, config):
        """
        Initialize the Load object with configuration parameters.

        Args:
            config (dict): The configuration dictionary containing required fields.

        Raises:
            ValueError: If any required configuration field is missing.
        """
        required_fields = ["provider", "destination_type", "load_destination", "path_prefix", "pipeline_name"]
        for field in required_fields:
            if field not in config:
                raise ValueError(f"Missing required configuration field: {field}")

        self.provider = config["provider"]
        self.load_destination = config["load_destination"]
        self.destination_type = config["destination_type"]
        self.path_prefix = config["path_prefix"]
        self.pipeline_name = config["pipeline_name"]
        self._creds_config = config.get("creds_config", {})

        # Initialize logging
        self.logger = logging.getLogger(f"load_pipeline.{self.pipeline_name}")

    def _get_creds(self):
        """
        Retrieve credentials based on the provider.

        Raises:
            NotImplementedError: If the provider is not supported.
        """
        if self.provider == "aws":
            # Implement logic to access AWS credentials from KMS
            # ...
            # Update with your specific implementation
            # return retrieved credentials
            return {}
        elif self.provider == "gcp":
            # Implement logic to access GCP credentials from Secret Manager
            # ...
            # Update with your specific implementation
            # return retrieved credentials
            return {}
        else:
            raise NotImplementedError(f"Provider {self.provider} not supported.")

    def get_target_path(self):
        """
        Construct the target path based on the current date and path prefix.

        Returns:
            str: The target path for data loading.
        """
        return f"{self.load_destination}/{datetime.now().strftime(self.path_prefix)}"

    def prepare_data_source(self):
        """
        Access and prepare data from the source using the retrieved credentials.

        Raises:
            Exception: Any errors encountered while accessing the data source.

        Returns:
            object: The prepared data object specific to your data source.
        """
        # Implement logic to access and prepare data based on provider and credentials
        # ...
        # Update with your specific data source and processing steps
        # raise exceptions if needed
        # return the prepared data object

    def write_data(self, data, target_path):
        """
        Write the prepared data to the specified target path.

        Args:
            data: The data object to be written.
            target_path (str): The path to write the data to.

        Raises:
            Exception: Any errors encountered while writing data.
        """
        # Implement logic to write data to the target path using the data object
        # ...
        # Update with your specific data writing method and error handling
        # raise exceptions if needed

    def load(self):
        try:
            # Get credentials
            credentials = self._get_creds()

            # Prepare data source
            data_source = self.prepare_data_source()

            # Get target path
            target_path = self.get_target_path()

            # Track progress (optional)
            progress_counter = 0

            # Write data in batches or loop through records (depending on your data source)
            for record in data_source:
                self.write_data(record, target_path)
                progress_counter += 1
                self.logger.info(f"Processed record {progress_counter}")

            self.logger.info(f"Successfully loaded {progress_counter} records to {target_path}")
        except Exception as e:
            self.logger.error(f"Error loading data: {e}")

        # Handle any additional cleanup or resource release