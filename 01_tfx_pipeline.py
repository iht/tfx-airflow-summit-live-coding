import os

from datetime import datetime
from typing import Dict, List

import tfx.v1 as tfx
from tfx.components import CsvExampleGen
from tfx.orchestration import metadata
from tfx.extensions.google_cloud_big_query.example_gen.component import BigQueryExampleGen
from tfx.orchestration.airflow.airflow_dag_runner import AirflowDagRunner


def create_pipeline(data_location: str,
                    query: str,
                    pipeline_name: str,
                    pipeline_root: str,
                    preprocessing_location: str,
                    beam_args: List[str]) -> tfx.dsl.Pipeline:
    # example_gen: CsvExampleGen = CsvExampleGen(input_base=data_location)
    example_gen = BigQueryExampleGen(query=query)

    statistics_gen = tfx.components.StatisticsGen(examples=example_gen.outputs['examples'])
    schema_gen = tfx.components.SchemaGen(statistics=statistics_gen.outputs['statistics'])

    transform = tfx.components.Transform(examples=example_gen.outputs['examples'],
                                         module_file=preprocessing_location,
                                         schema=schema_gen.outputs['schema'])

    components = [example_gen, statistics_gen, schema_gen, transform]

    metadata_path = os.path.join(pipeline_root, "tfx_metadata.db")
    metadata_conn = metadata.sqlite_metadata_connection_config(metadata_db_uri=metadata_path)

    pipeline = tfx.dsl.Pipeline(pipeline_name=pipeline_name,
                                pipeline_root=pipeline_root,
                                metadata_connection_config=metadata_conn,
                                components=components,
                                enable_cache=True,
                                beam_pipeline_args=beam_args)

    return pipeline


def run_pipeline(data_location: str,
                 query: str,
                 pipeline_name: str,
                 pipeline_root: str,
                 preprocessing_location: str,
                 beam_args: List[str]):
    p: tfx.dsl.Pipeline = create_pipeline(data_location=data_location,
                                          query=query,
                                          pipeline_name=pipeline_name,
                                          pipeline_root=pipeline_root,
                                          preprocessing_location=preprocessing_location,
                                          beam_args=beam_args)
    airflow_config = {
        'schedule_interval': None,
        'start_date': datetime(2022, 5, 1)}

    runner = AirflowDagRunner(config=airflow_config)

    return runner.run(p)


PIPELINE_NAME = "01_my_first_tfx_pipeline"
PIPELINE_ROOT = "/tmp/airflow-summit-live/tfx/"
DATA_LOCATION = "/Users/ihr/projects/tfx-airflow-summit-live-coding/data/"
PREPROCESSING_LOCATION = "/Users/ihr/projects/tfx-airflow-summit-live-coding/preprocessing_fn.py"

BEAM_ARGS_LOCAL = ["--runner=DirectRunner"]

QUERY = "SELECT * FROM `bigquery-public-data.ml_datasets.iris`"

PIPELINE_ROOT_CLOUD = "gs://tfx-airflow-summit-2022/pipelines/"
PREPROCESSING_LOCATION_CLOUD = "gs://tfx-airflow-summit-2022/code/preprocessing_fn.py"
TEMP_LOCATION = "gs://tfx-airflow-summit-2022/tmp/"

SERVICE_ACCOUNT = "tfx-sa@tfx-airflow-summit-2022.iam.gserviceaccount.com"

BEAM_ARGS_CLOUD = ["--runner=DataflowRunner",
                   "--project=tfx-airflow-summit-2022",
                   f"--temp_location={TEMP_LOCATION}",
                   "--region=europe-west3",
                   f"--service_account_email={SERVICE_ACCOUNT}",
                   "--subnetwork=regions/europe-west3/subnetworks/default",
                   "--no_use_public_ips",
                   "--dataflow_service_options=enable_prime"]

DAG = run_pipeline(data_location=DATA_LOCATION,
                   query=QUERY,
                   pipeline_root=PIPELINE_ROOT_CLOUD,
                   pipeline_name=PIPELINE_NAME,
                   preprocessing_location=PREPROCESSING_LOCATION_CLOUD,
                   beam_args=BEAM_ARGS_CLOUD)
