from abc import ABC, abstractmethod
import functools

# Configure logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# 🔹 Data Optimizer Decorator
def DataOptimizer(func):
    """Decorator to apply data optimization before and after function execution."""
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            logging.info(f"Optimizing before executing {func.__name__}...")
            self._optimize_data()  # Apply optimization before function runs
            
            result = func(self, *args, **kwargs)  # Execute function
            
            logging.info(f"Optimizing after executing {func.__name__}...")
            self._optimize_data()  # Apply optimization after function runs
            return result
        except Exception as e:
            logging.error(f"Error in {func.__name__}: {e}")
            return None
    return wrapper


# 🔹 Abstract Base Class
class BaseDataProcessor(ABC):
    """Abstract base class for data processing tasks."""

    def __init__(self, dataframe: pd.DataFrame):
        self._df = dataframe  # Encapsulation (Data Hiding)

    @abstractmethod
    def process(self):
        """Abstract method that must be implemented by subclasses."""
        pass

    def get_data(self):
        """Returns the processed DataFrame."""
        return self._df


# 🔹 Data Cleaning Class
class DataCleaning(BaseDataProcessor):
    """Handles data cleaning tasks such as missing values and duplicates."""

    def process(self):
        """Runs all cleaning operations."""
        self.check_missing_values()
        self.check_duplicate_values()
        self.remove_duplicate_values()

    @DataOptimizer
    def check_missing_values(self):
        """Logs and returns missing values count per column."""
        missing_values = self._df.isnull().sum()
        logging.info("Checked for missing values.")
        return missing_values[missing_values > 0]

    @DataOptimizer
    def check_duplicate_values(self):
        """Logs and returns the number of duplicate rows."""
        duplicate_count = self._df.duplicated().sum()
        logging.info(f"Found {duplicate_count} duplicate rows.")
        return duplicate_count

    @DataOptimizer
    def impute_median_missing_values(self, column: str):
        """Imputes missing values with the median of the column."""
        if column in self._df.columns:
            median_value = self._df[column].median()
            self._df[column].fillna(median_value, inplace=True)
            logging.info(f"Imputed missing values in '{column}' with median: {median_value}")
        else:
            logging.warning(f"Column '{column}' not found in DataFrame.")

    @DataOptimizer
    def remove_duplicate_values(self):
        """Removes duplicate rows from the dataset."""
        initial_count = len(self._df)
        self._df.drop_duplicates(inplace=True)
        removed_count = initial_count - len(self._df)
        logging.info(f"Removed {removed_count} duplicate rows.")

    def _optimize_data(self):
        """Internal method to optimize data (hidden from user)."""
        try:
            self._df = self._df.convert_dtypes()  # Convert data types for memory efficiency
            logging.info("Data optimized: Applied data type conversion.")
        except Exception as e:
            logging.error(f"Error optimizing data: {e}")


# 🔹 Example Usage
if __name__ == "__main__":
    try:
        df = pd.read_csv("sample_data.csv")
        cleaner = DataCleaning(df)
        cleaner.process()  # Run all cleaning tasks
        cleaned_df = cleaner.get_data()
        logging.info("Data cleaning completed successfully.")
    except Exception as e:
        logging.error(f"Critical error in data cleaning: {e}")
