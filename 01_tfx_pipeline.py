import os

from datetime import datetime

import tfx.v1 as tfx
from tfx.components import CsvExampleGen
from tfx.orchestration import metadata
from tfx.extensions.google_cloud_big_query.example_gen.component import BigQueryExampleGen
from tfx.orchestration.airflow.airflow_dag_runner import AirflowDagRunner


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


def run_pipeline(data_location: str,
                 pipeline_name: str,
                 pipeline_root: str):
    p: tfx.dsl.Pipeline = create_pipeline(data_location=data_location,
                                          pipeline_name=pipeline_name,
                                          pipeline_root=pipeline_root)
    airflow_config = {
        'schedule_interval': None,
        'start_date': datetime(2022, 5, 1)}

    runner = AirflowDagRunner(config=airflow_config)

    return runner.run(p)


PIPELINE_NAME = "01_my_first_tfx_pipeline"
PIPELINE_ROOT = "/tmp/airflow-summit-live/tfx/"
DATA_LOCATION = "/Users/ihr/projects/tfx-airflow-summit-live-coding/data/iris_dataset.csv"

DAG = run_pipeline(data_location=DATA_LOCATION,
                   pipeline_root=PIPELINE_ROOT,
                   pipeline_name=PIPELINE_NAME)
