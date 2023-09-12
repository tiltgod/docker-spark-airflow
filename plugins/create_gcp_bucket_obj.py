from google.cloud import storage

def create_gcp_bucket_obj(gcp_bucket_credential, gcp_bucket_name):
    bucket_client = storage.Client.from_service_account_info(gcp_bucket_credential)
    gcp_bucket_obj = storage.Bucket(bucket_client, gcp_bucket_name)
    return gcp_bucket_obj