from bigquery_loader import BigQueryLoader

project_id = 'your-project-id'
dataset_id = 'your-dataset-id'
table_id = 'your-table-id'
credentials_path = 'path_to_your_credentials_file'

file_path = 'your_data_file.csv'
df = pd.read_csv(file_path)

bigquery_loader = BigQueryLoader(project_id, dataset_id, table_id, credentials_path)
bigquery_loader.load_data_in_batches(df, batch_size=50000, parallelism=4)
