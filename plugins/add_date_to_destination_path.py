import datetime
import os

def add_date_to_destination_path(source_path):
    my_datetime = datetime.datetime.today()
    my_date = my_datetime.date()
    my_date.strftime("%Y%d%m")
    # get only file name from path
    os.path.basename(source_path)
    bucket_desination =  my_date.strftime("%Y%d%m") +"_"+ os.path.basename(source_path)
    return bucket_desination