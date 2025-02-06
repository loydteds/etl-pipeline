def auto_optimize(func):
    """Decorator that applies data optimization after each method."""
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        result = func(self, *args, **kwargs)
        if isinstance(result, pd.DataFrame):
            logging.info(f"Automatically optimizing after {func.__name__}...")
            optimizer = DataOptimization(result)
            optimizer.downcast_integer()
            optimizer.downcast_float()
            optimizer.bitwise_categorical_data()
            return optimizer.get_optimized_data()
        return result
    return wrapper
