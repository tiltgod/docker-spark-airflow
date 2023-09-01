from dataclasses import dataclass
from gcp_connection_setting import GCPConnectionSettings
from to_gcs_setting import ToGCSSetting
from airflow.models import Variable

connection_config = Variable.get("gcp_connection", deserialize_json=True)
upload_config = Variable.get("upload_to_gcs", deserialize_json=True)

@dataclass
class DefaultSampleDag:
    gcp_connection_setting: GCPConnectionSettings
    to_gcs_setting: ToGCSSetting
    
default_config = DefaultSampleDag(
    gcp_connection_setting = GCPConnectionSettings(
        conn_id = connection_config["conn_id"],
        conn_type = connection_config["conn_type"],
        scope = connection_config["scope"],
        project = connection_config["project"],
        key_path = connection_config["key_path"]
    ),
    to_gcs_setting = ToGCSSetting(
        source_path = upload_config["source_path"],
        destination_path = upload_config["destination_path"],
        bucket_name = upload_config["bucket_name"]
    )
)