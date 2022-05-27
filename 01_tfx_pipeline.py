import os

import tfx.v1 as tfx
from tfx.components import CsvExampleGen
from tfx.orchestration import metadata
from tfx.extensions.google_cloud_big_query.example_gen.component import BigQueryExampleGen


def create_pipeline(data_location: str,
                    pipeline_name: str,
                    pipeline_root: str) -> tfx.dsl.Pipeline:
    example_gen: CsvExampleGen = CsvExampleGen(input_base=data_location)

    components = [example_gen]

    metadata_path = os.path.join(pipeline_root, "tfx_metadata.db")
    metadata_conn = metadata.sqlite_metadata_connection_config(metadata_db_uri=metadata_path)

    pipeline = tfx.dsl.Pipeline(pipeline_name=pipeline_name,
                                pipeline_root=pipeline_root,
                                metadata_connection_config=metadata_conn,
                                components=components,
                                enable_cache=True)

    return pipeline


def run_pipeline():
    pass


DAG = "something"
