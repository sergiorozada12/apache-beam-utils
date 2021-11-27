import apache_beam as beam

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from typing import Dict


class ESConnector(beam.DoFn):
    def __init__(self, host: str, port: int, scheme: str, timeout: int) -> None:
        self.host = host
        self.port = port
        self.scheme = scheme
        self.timeout = timeout

    def start_bundle(self) -> None:
        self.es_client = Elasticsearch([self.host], scheme=self.scheme, port=self.port, timeout=self.timeout)
        self.buffer = list()

    def finish_bundle(self) -> None:
        if self.buffer:
            self.dump()
        self.buffer = list()


class ESDummyWriter(ESConnector):
    def __init__(self, host: str, port: int, scheme: str, timeout:int, index: str, batch_size: int) -> None:
        super().__init__(host, port, scheme, timeout)
        self.batch_size = batch_size
        self.index = index

    def process(self, data: Dict) -> None:
        self.buffer.append(data)
        if len(self.buffer) >= self.batch_size:
            self.dump()

    def dump(self) -> None:
        bulk(
            self.es_client,
            self.buffer,
            index=self.index,
            raise_on_error=True
        )
        self.buffer = list()