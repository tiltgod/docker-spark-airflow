from google.cloud import storage

class GCSWrapper():
    
    credential_info = {}
    client = None
    __wrapper = None
    
    @staticmethod
    def getWrapper(credential_info):
        if GCSWrapper.__wrapper is None:
            GCSWrapper(credential_info)
        return GCSWrapper.__wrapper

    def __init__(self, credential_info):
        self.credential_info = credential_info
        self.location = "asia-southeast1"

        GCSWrapper.__wrapper = self

    def connect(self):
        self.client = storage.Client.from_service_account_info(self.credential_info)
        return self.client

    def upload_file_name(self, bucket_name: str, source_file: str, blob_name: str):
        try:
            bucket = self.get_bucket(bucket_name)
            blob = bucket.blob(blob_name)
            blob.upload_from_filename(source_file)
            return blob
        except:
            print(f"Could not upload file from {source_file} to gs://{bucket_name}/{blob_name}")
            raise
    
    def get_bucket(self, bucket_name):
        try:
            bucket = self.client.get_bucket(bucket_name)
            return bucket
        except:
            print(f"Could not get bucket '{bucket_name}'")
            raise

    def get_blob_list(self, bucket_name, prefix):
        blob_list = []
        try:
            bucket = self.get_bucket(bucket_name)
            blobs = bucket.list_blobs(prefix=prefix)

            for blob in blobs:
                blob_list.append(blob)

            return blob_list
        except Exception as e:
            err_msg = f'Could not get list of blob, {e}'
            print(err_msg, exec_info=True)
            raise Exception(err_msg)

    def download_blob_as_string(self, bucket_name, blob_name):
        bucket = self.get_bucket(bucket_name)
        blob = self.get_blob(bucket, blob_name)
        return blob.download_as_bytes().decode()
