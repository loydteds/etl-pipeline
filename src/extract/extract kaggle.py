import os
import kaggle

def download_kaggle_dataset(dataset_name: str, save_path: str = "data/raw"):
    """
    Downloads a dataset from Kaggle and saves it to the specified directory.
    
    :param dataset_name: Kaggle dataset identifier (e.g., "username/dataset-name")
    :param save_path: Directory where the dataset should be saved
    """
    os.makedirs(save_path, exist_ok=True)
    
    print(f"Downloading dataset {dataset_name}...")
    kaggle.api.dataset_download_files(dataset_name, path=save_path, unzip=True)
    print(f"Dataset downloaded and extracted to {save_path}")

if __name__ == "__main__":

    KAGGLE_DATASET = "username/dataset-name"
    download_kaggle_dataset(KAGGLE_DATASET)
