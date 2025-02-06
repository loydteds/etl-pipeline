from concurrent.futures import ThreadPoolExecutor
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO)

class DataOptimization:
    """Class to optimize data before loading to BigQuery."""

    def __init__(self, dataframe: pd.DataFrame):
        """Initialize with a DataFrame."""
        self._df = dataframe

    def batch_data(self, batch_size: int):
        """Batch the data for optimized loading."""
        total_rows = len(self._df)
        num_batches = (total_rows // batch_size) + (1 if total_rows % batch_size != 0 else 0)
        
        logging.info(f"Batching data into {num_batches} batches.")

        # Yielding each batch of data as separate DataFrame
        for i in range(num_batches):
            yield self._df.iloc[i * batch_size: (i + 1) * batch_size]

    def compress_data(self, output_path: str):
        """Compress data to reduce file size for upload."""
        try:
            self._df.to_csv(output_path, compression='gzip', index=False)
            logging.info(f"Data compressed and saved to {output_path}.")
        except Exception as e:
            logging.error(f"Error compressing data: {e}")
            raise
