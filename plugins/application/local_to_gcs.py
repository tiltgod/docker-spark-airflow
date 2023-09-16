from connector.gcs import GCSWrapper
import datetime
import os

def add_date_to_files(source_path):

    my_datetime = datetime.datetime.today()
    my_date = my_datetime.date()
    files_name_lst = os.listdir(source_path)

    for file_name in files_name_lst:
        ex_path = source_path + "/" + file_name
        file_added_date = source_path + "/" + my_date.strftime("%Y%d%m") + file_name
        os.rename(ex_path, file_added_date)
    
    files_name_lst = os.listdir(source_path)
    return files_name_lst

def local_to_gcs(credential_info, bucket_name, local_dir):

    # create gcs connectoin
    gcs_client = GCSWrapper.getWrapper(credential_info)
    gcs_client.connect()

    # add date to each files name
    file_name_lst = add_date_to_files(local_dir)

    # upload files to bucket
    for file in file_name_lst:
        source_file_path = local_dir + file
        blob_name = "test" + "/" + file
        gcs_client.upload_file_name(bucket_name, source_file_path, blob_name)