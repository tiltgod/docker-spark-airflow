from connector.gcs import GCSWrapper
from file_utils import add_date_to_files

def local_to_gcs(credential_info, bucket_name, local_dir):

    # create connectoin
    gcs_client = GCSWrapper.getWrapper(credential_info)
    gcs_client.connect()

    # add date to each files name
    file_name_lst = add_date_to_files(local_dir)

    # upload files to bucket
    for file in file_name_lst:
        source_file_path = local_dir + file
        blob_name = "test" + "/" + file
        gcs_client.upload_file_name(bucket_name, source_file_path, blob_name)