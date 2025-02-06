import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DataCleaning:
    def __init__(self, dataframe: pd.DataFrame):
        self._df = dataframe  # Encapsulation (data hiding)
    
    def check_missing_values(self):
        """Logs and returns missing values count per column."""
        try:
            missing_values = self._df.isnull().sum()
            logging.info("Checked for missing values.")
            return missing_values[missing_values > 0]
        except Exception as e:
            logging.error(f"Error checking missing values: {e}")
            return None

    def check_duplicate_values(self):
        """Logs and returns the number of duplicate rows."""
        try:
            duplicate_count = self._df.duplicated().sum()
            logging.info(f"Found {duplicate_count} duplicate rows.")
            return duplicate_count
        except Exception as e:
            logging.error(f"Error checking duplicate values: {e}")
            return None
    
    def impute_median_missing_values(self, column: str):
        """Imputes missing values with the median of the column."""
        try:
            if column in self._df.columns:
                median_value = self._df[column].median()
                self._df[column].fillna(median_value, inplace=True)
                logging.info(f"Imputed missing values in '{column}' with median: {median_value}")
            else:
                logging.warning(f"Column '{column}' not found in DataFrame.")
        except Exception as e:
            logging.error(f"Error imputing missing values in '{column}': {e}")
    
    def remove_duplicate_values(self):
        """Removes duplicate rows from the dataset."""
        try:
            initial_count = len(self._df)
            self._df.drop_duplicates(inplace=True)
            final_count = len(self._df)
            removed_count = initial_count - final_count
            logging.info(f"Removed {removed_count} duplicate rows.")
        except Exception as e:
            logging.error(f"Error removing duplicate values: {e}")

    def get_cleaned_data(self):
        """Returns the cleaned DataFrame."""
        return self._df

if __name__ == "__main__":
    try:
        df = pd.read_csv("sample_data.csv")
        cleaner = DataCleaning(df)
        cleaner.check_missing_values()
        cleaner.check_duplicate_values()
        cleaner.impute_median_missing_values("Sales")
        cleaner.remove_duplicate_values()
        cleaned_df = cleaner.get_cleaned_data()
        logging.info("Data cleaning completed successfully.")
    except Exception as e:
        logging.error(f"Critical error in data cleaning: {e}")
