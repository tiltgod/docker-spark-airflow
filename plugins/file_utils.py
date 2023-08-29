import datetime
import os

def add_date_to_files(source_path):

    my_datetime = datetime.datetime.today()
    my_date = my_datetime.date()

    files_name_lst = os.listdir(source_path)
    for file_name in files_name_lst:
        file_added_date = my_date.strftime("%Y%d%m") + file_name
        os.rename(source_path+"/"+file_name, source_path+"/"+file_added_date)
    
        

# def create_dir_schema(dir_path):
# create schema file yaml that has the list of file names of data_sorce's dir
