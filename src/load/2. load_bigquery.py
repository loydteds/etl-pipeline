from google.cloud import bigquery
from google.oauth2 import service_account
from concurrent.futures import ThreadPoolExecutor
from data_optimization import DataOptimization

logging.basicConfig(level=logging.INFO)

class BigQueryLoader:
    """Class for loading optimized data into BigQuery."""

    def __init__(self, project_id: str, dataset_id: str, table_id: str, credentials_path: str = None):
        """Initialize with BigQuery connection details."""
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.credentials_path = credentials_path
        self.client = self._initialize_client()

    def _initialize_client(self):
        """Initialize the BigQuery client."""
        try:
            if self.credentials_path:
                credentials = service_account.Credentials.from_service_account_file(self.credentials_path)
                client = bigquery.Client(credentials=credentials, project=self.project_id)
            else:
                client = bigquery.Client(project=self.project_id)
            logging.info("BigQuery client initialized successfully.")
            return client
        except Exception as e:
            logging.error(f"Error initializing BigQuery client: {e}")
            raise

    def _load_to_bigquery(self, data_chunk: pd.DataFrame):
        """Helper method to load data chunk to BigQuery."""
        try:
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                source_format=bigquery.SourceFormat.CSV,
                autodetect=True
            )

            job = self.client.load_table_from_dataframe(
                data_chunk, f"{self.project_id}.{self.dataset_id}.{self.table_id}", job_config=job_config
            )
            job.result()  # Wait for the job to complete
            logging.info("Batch data loaded to BigQuery successfully.")
        except Exception as e:
            logging.error(f"Error loading batch data: {e}")
            raise

    def load_data_in_batches(self, dataframe: pd.DataFrame, batch_size: int = 100000, parallelism: int = 4):
        """Load data in parallel batches to BigQuery."""
        try:
            # Initialize DataOptimization
            data_opt = DataOptimization(dataframe)
            data_opt.trim_whitespaces()
            data_opt.normalize_numeric_data()
            
            # Batch data before uploading
            batches = list(data_opt.batch_data(batch_size))

            # Use ThreadPoolExecutor to load batches in parallel
            with ThreadPoolExecutor(max_workers=parallelism) as executor:
                executor.map(self._load_to_bigquery, batches)
            logging.info("Data loading complete.")
        except Exception as e:
            logging.error(f"Error loading data in batches: {e}")
            raise
