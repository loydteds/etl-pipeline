import logging
from multiprocessing import cpu_count, Pool

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class DataOptimization:
    def __init__(self, df: pd.DataFrame):
        self._df = df

    def batch_processing(self, batch_size=1000):
        """Process data in batches to optimize memory usage."""
        try:
            num_batches = len(self._df) // batch_size + (1 if len(self._df) % batch_size else 0)
            for i in range(num_batches):
                batch = self._df.iloc[i * batch_size: (i + 1) * batch_size]
                logging.info(f"Processing batch {i+1}/{num_batches}")
                yield batch
        except Exception as e:
            logging.error(f"Error in batch processing: {e}")
            return None

    def parallel_processing(self, func, num_workers=cpu_count()):
        """Apply a function to the DataFrame using parallel processing."""
        try:
            with Pool(processes=num_workers) as pool:
                results = pool.map(func, np.array_split(self._df, num_workers))
            self._df = pd.concat(results, ignore_index=True)
            logging.info("Parallel processing completed successfully.")
        except Exception as e:
            logging.error(f"Error in parallel processing: {e}")

    def identify_columns(self):
        """Identify column data types for optimization."""
        try:
            column_info = self._df.dtypes.to_dict()
            logging.info("Column types identified successfully.")
            return column_info
        except Exception as e:
            logging.error(f"Error in identifying columns: {e}")
            return None

    def downcast_integer(self):
        """Downcast integer columns to reduce memory usage."""
        try:
            int_cols = self._df.select_dtypes(include=['int']).columns
            for col in int_cols:
                self._df[col] = pd.to_numeric(self._df[col], downcast="integer")
            logging.info("Integer columns downcasted successfully.")
        except Exception as e:
            logging.error(f"Error in downcasting integers: {e}")

    def downcast_float(self):
        """Downcast float columns to reduce memory usage."""
        try:
            float_cols = self._df.select_dtypes(include=['float']).columns
            for col in float_cols:
                self._df[col] = pd.to_numeric(self._df[col], downcast="float")
            logging.info("Float columns downcasted successfully.")
        except Exception as e:
            logging.error(f"Error in downcasting floats: {e}")

    def bitwise_categorical_data(self):
        """Convert categorical data into more memory-efficient representations."""
        try:
            cat_cols = self._df.select_dtypes(include=['object']).columns
            for col in cat_cols:
                self._df[col] = self._df[col].astype('category')
            logging.info("Categorical columns optimized successfully.")
        except Exception as e:
            logging.error(f"Error in optimizing categorical data: {e}")

    def get_optimized_data(self):
        """Return the optimized DataFrame."""
        return self._df
