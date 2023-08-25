import json
from airflow import settings
from airflow.models import Connection

def create_gcp_connection(gcp_connection_config):
    print(type(gcp_connection_config))
    print(gcp_connection_config)
    new_conn = Connection(
            conn_id=gcp_connection_config.conn_id,
            conn_type=gcp_connection_config.conn_type,
    )
    extra_field = {
        "extra__google_cloud_platform__scope": gcp_connection_config.scope,
        "extra__google_cloud_platform__project": gcp_connection_config.project,
        "extra__google_cloud_platform__key_path": gcp_connection_config.keypath
    }

    session = settings.Session()

    #checking if connection exist
    if session.query(Connection).filter(Connection.conn_id == new_conn.conn_id).first():
        my_connection = session.query(Connection).filter(Connection.conn_id == new_conn.conn_id).one()
        my_connection.set_extra(json.dumps(extra_field))
        session.add(my_connection)
        session.commit()
    else: #if it doesn't exit create one
        new_conn.set_extra(json.dumps(extra_field))
        session.add(new_conn)
        session.commit()