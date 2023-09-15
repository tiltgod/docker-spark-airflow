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


