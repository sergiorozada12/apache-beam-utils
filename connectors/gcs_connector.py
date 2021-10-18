import os
import json

from typing import Dict

import apache_beam as beam
from google.cloud import storage


class GCSConnector(beam.DoFn):
    def __init__(
        self,
        project_id: str,
        bucket_name: str,
        content_path: str
    ) -> None:
        self.project_id = project_id
        self.bucket_name = bucket_name
        self.content_path = content_path

    def start_bundle(self) -> None:
        self.storage_client = storage.Client(project=self.project_id)
        self.bucket = storage.bucket.Bucket(client=self.storage_client, name=self.bucket_name)


class GCSDummyJsonReader(GCSConnector):
    def process(self, filename_json: str) -> Dict:
        path = os.path.join(self.content_path, filename_json)
        blob = self.bucket.blob(path)
        if blob.exists():
            yield json.loads(blob.download_as_string())


class GCSDummyJsonWriter(GCSConnector):
    def process(self, filename_json: str, dictionary: Dict) -> None:
        path = os.path.join(self.content_path, filename_json)
        blob = self.bucket.blob(path)
        blob.upload_from_string(data=json.dumps(dictionary), content_type='application/json')
