"""
ideally design

config might looks like this
{
    Transform: {
        column_name: do_certain_transformation

    },
}

"""
import hashlib

class Transform:
    def __init__(self, transform_config):
        self.transform_config = transform_config
        self.transform_methods = {
            "encoding": self._do_encoding,
            "encryption": self._do_encryption,
            "aggregate": self._do_aggregate,
            "filter": self._do_filtering,
            "split_file": self._do_splitting_file,
        }
    def _do_encoding(self, data):
        # Implement encoding logic here
        return transformed_data

    def _do_encryption(self, data):
        # Implement encryption logic here
        encrypt_method = {
            "sha256" : hashlib.sha256,
            "sha512" : hashlib.sha512

        }
        return transformed_data

    def _do_aggregate(self, data):
        # Implement aggregation logic here
        # ...
        return transformed_data

    def _do_filtering(self, data):
        # Implement filtering logic here
        # ...
        return filtered_data

    def _do_splitting_file(self, data):
        # Implement file splitting logic here
        # ...
        return split_files_list

    def transform(self, data):
        transformed_data = {}
        for column_name, transformation in self.transform_config.items():
            try:
                transform_func = self.transform_methods.get(transformation, None)
                if not transform_func:
                    raise ValueError(f"Unknown transformation: {transformation}")
                transformed_data[column_name] = transform_func(data[column_name])
            except Exception as e:
                # Handle specific error for each transformation
                # ...
                raise

        return transformed_data
