from dataclasses import dataclass
from gcp_connection_setting import GCPConnectionSettings
from airflow.models import Variable

#connection_config = Variable.get("test", deserialize_json=True) 
connection_config = Variable.get("gcp_connection", deserialize_json=True)

@dataclass
class DefaultSampleDag:
    gcp_connection_setting: GCPConnectionSettings
    
default_config = DefaultSampleDag(
    gcp_connection_setting = GCPConnectionSettings(
        # conn_id = test["test1"], #  ca
        conn_id = connection_config["conn_id"],
        conn_type = connection_config["conn_type"],
        scope = connection_config["scope"],
        project = connection_config["project"],
        keypath = connection_config["keypath"]
    )
)