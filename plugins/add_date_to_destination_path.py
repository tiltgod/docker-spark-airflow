import datetime
import os

def add_date_to_destination_path(src_file_name):
    my_datetime = datetime.datetime.today()
    my_date = my_datetime.date()
    # get only file name from path
    _file_name = os.path.basename(src_file_name).split(".")
    file_name = _file_name[0]
    file_extension = _file_name[1]
    des_file_name =  f"{file_name}_{my_date.strftime('%Y%m%d')}.{file_extension}"
    return des_file_name

print(add_date_to_destination_path("\program\dog.json"))
