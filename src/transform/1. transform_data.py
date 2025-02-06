class DataTransformer:
    """Handles data transformation tasks like standardizing column names."""
    
    def __init__(self, data_frame: pd.DataFrame):
        self._df = data_frame

    def place_underscore(self):
        """Replaces spaces in column names with underscores."""
        try:
            self._df.columns = self._df.columns.str.replace(' ', '_')
            logging.info("Spaces in column names replaced with underscores.")
        except Exception as e:
            logging.error(f"Error placing underscores in column names: {e}")
            raise
    
    def lowercase_columns(self):
        """Converts all column names to lowercase."""
        try:
            self._df.columns = self._df.columns.str.lower()
            logging.info("Column names converted to lowercase.")
        except Exception as e:
            logging.error(f"Error converting column names to lowercase: {e}")
            raise
    
    def get_transformed_data(self):
        """Returns the transformed DataFrame."""
        return self._df
