class ValidationBase:
    """Base class for data validation tasks."""
    
    def __init__(self, data_frame: pd.DataFrame):
        self._df = data_frame

    def validate(self):
        """Perform column validation. Must be implemented in subclasses."""
        raise NotImplementedError("This method must be implemented in subclasses.")

    def _log_invalid(self, column, issue):
        """Helper method to log invalid columns/issues."""
        logging.warning(f"Column: {column}, Issue: {issue}")


class NumericValidator(ValidationBase):
    """Validator for numeric columns."""
    
    def validate(self):
        invalid_values = []
        for column in self._df.columns:
            column_values = self._df[column]
            if column_values.dtype in ['int64', 'float64']:  # Numeric column
                if self._check_numeric_values(column_values):
                    invalid_values.append((column, 'Invalid numeric values found'))
        return invalid_values

    def _check_numeric_values(self, column_values):
        """Check if all values in a numeric column are valid."""
        invalid_values = column_values[~column_values.apply(pd.to_numeric, errors='coerce').notnull()]
        return not invalid_values.empty


class CategoricalValidator(ValidationBase):
    """Validator for categorical columns."""
    
    def validate(self):
        invalid_values = []
        for column in self._df.columns:
            column_values = self._df[column]
            if column_values.dtype == 'object':  # Categorical column (non-numeric)
                if self._check_categorical_values(column_values):
                    invalid_values.append((column, 'Invalid categorical values found'))
        return invalid_values

    def _check_categorical_values(self, column_values):
        """Check if all values in a categorical column are valid."""
        invalid_values = column_values[column_values.apply(lambda x: isinstance(x, str) and x.isnumeric())]
        return not invalid_values.empty


class DataValidator:
    """Handles data validation tasks while managing data processing operations."""
    
    def __init__(self, data_frame: pd.DataFrame):
        self._df = data_frame

    def save_to_csv(self, data, filename):
        """Helper function to save DataFrame to CSV."""
        try:
            data.to_csv(filename, index=False)
            logging.info(f"Data saved to {filename}")
        except Exception as e:
            logging.error(f"Error saving data to {filename}: {e}")
            raise
    
    def process(self):
        """Runs all validation operations."""
        try:
            validation_results = self._validate_columns()
            if validation_results:
                invalid_df = pd.DataFrame(validation_results, columns=['Column', 'Issue'])
                self.save_to_csv(invalid_df, "invalid_values.csv")
            logging.info("Data validation process completed.")
        except Exception as e:
            logging.error(f"Error during data validation: {e}")
            raise
    
    def _validate_columns(self):
        """Validate each column in the DataFrame."""
        invalid_values = []
        
        # Use different validators for numeric and categorical columns
        numeric_validator = NumericValidator(self._df)
        categorical_validator = CategoricalValidator(self._df)

        invalid_values.extend(numeric_validator.validate())
        invalid_values.extend(categorical_validator.validate())
        
        return invalid_values
