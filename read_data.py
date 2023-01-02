import ndjson
from google.cloud import storage

def extract_data(bucket_name, file_name):
# Gets the JSON formatted data from Google Cloud Storage and loads the data to a list
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.get_blob(file_name)
    json_list = ndjson.loads(blob.download_as_string())

    return json_list