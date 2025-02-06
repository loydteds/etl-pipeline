logging.basicConfig(level=logging.INFO)

class DataTransformer:
    """Base class for all data transformation operations."""

    def __init__(self, data_frame: pd.DataFrame):
        self._df = data_frame

    def _log_error(self, message, exception):
        """Helper function to log errors consistently."""
        logging.error(f"{message}: {exception}")
        raise

    def apply_transformation(self, transformation_method, *args, **kwargs):
        """Applies a transformation method while handling exceptions."""
        try:
            transformation_method(*args, **kwargs)
        except Exception as e:
            self._log_error(f"Error applying transformation {transformation_method.__name__}", e)

class DataCleaner(DataTransformer):
    """Class for cleaning and transforming data."""

    def trim_whitespaces(self):
        """Removes leading/trailing spaces in both column names and string values."""
        try:
            self._df.columns = self._df.columns.str.strip()  # Trim column names
            self._df = self._df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)  # Trim values
            logging.info("Leading/trailing whitespaces removed.")
        except Exception as e:
            self._log_error("Error trimming whitespaces", e)

    def normalize_numeric_data(self):
        """Normalizes numeric columns to a range [0, 1] using min-max scaling."""
        try:
            numeric_columns = self._df.select_dtypes(include=['float64', 'int64']).columns
            self._df[numeric_columns] = self._df[numeric_columns].apply(lambda x: (x - x.min()) / (x.max() - x.min()))
            logging.info("Numeric data normalized.")
        except Exception as e:
            self._log_error("Error normalizing numeric data", e)

    def log_transform(self, column_name):
        """Applies log transformation to a numeric column."""
        try:
            self._df[column_name] = np.log1p(self._df[column_name])
            logging.info(f"Log transformation applied to {column_name}.")
        except Exception as e:
            self._log_error(f"Error applying log transformation to {column_name}", e)

    def date_transform(self, column_name):
        """Converts date column to datetime and extracts useful features."""
        try:
            self._df[column_name] = pd.to_datetime(self._df[column_name], errors='coerce')
            self._df[column_name + '_year'] = self._df[column_name].dt.year
            self._df[column_name + '_month'] = self._df[column_name].dt.month
            self._df[column_name + '_day'] = self._df[column_name].dt.day
            logging.info(f"Date transformation applied to {column_name}.")
        except Exception as e:
            self._log_error(f"Error transforming date column {column_name}", e)

class DataProcessor:
    """Class for managing and applying data transformations to a DataFrame."""
    
    def __init__(self, data_frame: pd.DataFrame):
        self.data_cleaner = DataCleaner(data_frame)

    def process_data(self):
        """Process data using transformations."""
        # Example transformations
        self.data_cleaner.apply_transformation(self.data_cleaner.trim_whitespaces)
        self.data_cleaner.apply_transformation(self.data_cleaner.normalize_numeric_data)
        self.data_cleaner.apply_transformation(self.data_cleaner.log_transform, 'column_name')  
        self.data_cleaner.apply_transformation(self.data_cleaner.date_transform, 'date_column')  
 
