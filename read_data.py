import ndjson
import ast
from google.cloud import storage

def extract_data(bucket_name, file_name):
# Gets the JSON formatted data from Google Cloud Storage and loads the data to a list
    #storage_client = storage.Client()
    #bucket = storage_client.get_bucket(bucket_name)
    #blob = bucket.get_blob(file_name)
    #json_list = ndjson.loads(blob.download_as_string())
    file_path = 'C:/Users/ytkna/Desktop/challenge_data.json'

    json_list = []

    with open(file_path, 'r') as f:
        data = f.readlines()

    for item in data:
        json_list.append(ast.literal_eval(item.replace('/n', '')))
    print(file_path)
    print(json_list)
    return json_list