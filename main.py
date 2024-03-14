import math
import pandas as pd
from datetime import datetime
import os
import threading
from threading import Thread

from config import dir_path, save_path, start, end, row_filter_column, file_suffix

filtered_df = pd.DataFrame()
lock = threading.Lock()

def multi_process_xlsx(_path_to_files, _row_filter_column, _start_date, _end_date):
    for _path_to_file in _path_to_files:
        print("processing file: " + _path_to_file)
        df = pd.read_excel(_path_to_file)
        for row in range(0, len(df)):
            selected_row = df.iloc[row]  # get row data, type: Pandas Series
            if type(selected_row[_row_filter_column]) == float and math.isnan(selected_row[_row_filter_column]):
                continue
            open_date = datetime.strptime(selected_row[_row_filter_column], '%Y-%m-%d')
            if _start_date <= open_date <= _end_date:
                lock.acquire()
                filtered_df = filtered_df.append(selected_row.transpose())
                lock.release()


def multi_thread_main():
     # load config
    start_date = datetime.strptime(start, '%Y-%m-%d')
    end_date = datetime.strptime(end, '%Y-%m-%d')
    if end_date < start_date:
        print('Error: end date is earlier than start date')
        exit(1)

    # get all directories in dir_path
    directory_names = [d for d in os.listdir(dir_path) if os.path.isdir(os.path.join(dir_path, d))]
    threads_num = 4
    for directory_name in directory_names:
        split_files = {}
        filtered_df = pd.DataFrame()

        pwd = os.path.join(dir_path, directory_name)
        files = [f for f in os.listdir(pwd) if f.endswith('.xlsx') and 'filtered' not in f]
        for i, file in enumerate(files):
            idx = i % threads_num
            if idx not in split_files:
                split_files[idx] = []
            split_files[idx].append(os.path.join(pwd, file))
        
        th0 = Thread(target=multi_process_xlsx, args=(split_files[0], row_filter_column, start_date, end_date))
        th1 = Thread(target=multi_process_xlsx, args=(split_files[1], row_filter_column, start_date, end_date))
        th2 = Thread(target=multi_process_xlsx, args=(split_files[2], row_filter_column, start_date, end_date))
        th3 = Thread(target=multi_process_xlsx, args=(split_files[3], row_filter_column, start_date, end_date))

        th0.start()
        th1.start()
        th2.start()
        th3.start()
        th0.join()
        th1.join()
        th2.join()
        th3.join()
         
        filtered_df.to_excel(os.path.join(dir_path, directory_name) + file_suffix + '.xlsx', index=False)
        print("file saved to: " + os.path.join(dir_path, directory_name) + file_suffix + '.xlsx')


if __name__ == '__main__':
    multi_thread_main()    
