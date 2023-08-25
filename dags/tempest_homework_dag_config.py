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
        keypath = connection_config["keypath"]
    ),
    to_gcs_setting = ToGCSSetting(
        src_name = upload_config["src_name"],
        dir_name = upload_config["dir_name"],
        bucket_name = upload_config["bucket_name"]
    )
)