import math
import pandas as pd
from datetime import datetime
import os

from config import dir_path, save_path, start, end, row_filter_column, file_suffix


def process_xlsx(_path_to_file, _row_filter_column, _start_date, _end_date):
    df = pd.read_excel(_path_to_file)
    new_df = pd.DataFrame()
    for row in range(0, len(df)):
        selected_row = df.iloc[row]  # get row data, type: Pandas Series
        if type(selected_row[_row_filter_column]) == float and math.isnan(selected_row[_row_filter_column]):
            continue
        open_date = datetime.strptime(selected_row[_row_filter_column], '%Y-%m-%d')
        if _start_date <= open_date <= _end_date:
            new_df = new_df._append(selected_row.transpose())
    return new_df


if __name__ == '__main__':
    # load config
    start_date = datetime.strptime(start, '%Y-%m-%d')
    end_date = datetime.strptime(end, '%Y-%m-%d')
    if end_date < start_date:
        print('Error: end date is earlier than start date')
        exit(1)

    # get all directories in dir_path
    directory_names = [d for d in os.listdir(dir_path) if os.path.isdir(os.path.join(dir_path, d))]

    # iterate through first level directories
    for directory_name in directory_names:
        pwd = os.path.join(dir_path, directory_name)
        print("entering directory: " + pwd)

        # get all xlsx files in pwd
        files = [f for f in os.listdir(pwd)
                 if f.endswith('.xlsx') and 'filtered' not in f]  # TODO delete 't' for production env
        print("Excel files in directory: " + str(files))

        # read xlsx file by file ,filter data and save to new DataFrame
        filtered_df = pd.DataFrame()
        for file in files:
            path_to_file = os.path.join(pwd, file)
            print("processing file: " + path_to_file)
            filtered_df = (
                filtered_df._append(process_xlsx(path_to_file, row_filter_column, start_date, end_date)))
        if save_path == 'default':
            save_path = dir_path
        filtered_df.to_excel(os.path.join(save_path, directory_name) + file_suffix + '.xlsx', index=False)
        print("file saved to: " + os.path.join(save_path, directory_name) + file_suffix + '.xlsx')
    pass
