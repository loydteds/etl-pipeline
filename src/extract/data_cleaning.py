class DataCleaning(DataOptimization):
    """Handles data cleaning tasks while inheriting from DataOptimization."""

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

if __name__ == "__main__":
    try:
        df = pd.read_csv("sample_data.csv")
        cleaner = DataCleaning(df)
        cleaner.process()  # Run all cleaning tasks
        cleaned_df = cleaner.get_optimized_data()
        logging.info("Data cleaning completed successfully.")
    except Exception as e:
        logging.error(f"Critical error in data cleaning: {e}")
