import json
import sys

from google.cloud import storage

class DataManagement:

    def __init__(self, project_id, bucket_id):
        self.project_id = project_id
        self.bucket_id = bucket_id
        self.client = storage.Client(project=self.project_id)
        self.bucket = self.client.get_bucket(self.bucket_id)


    def store_json(self, file, file_name):
        # Configure blob
        blob = self.bucket.blob(file_name)

        print(type(file), file=sys.stdout)
        sys.stdout.flush()
                
        # Upload the locally saved model
        blob.upload_from_string(str(file), content_type='application/json')

    def fetch_json(self, file_name, path):
        output_path = '{0}/{1}'.format(file_name, path)
        blob = self.bucket.blob(file_name)
        blob.download_to_filename(output_path)
        