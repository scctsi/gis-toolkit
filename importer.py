import os
import pathlib
import pandas as pd


def import_file(full_file_path):
    input_data_frame = None

    if not (os.path.isfile(full_file_path)):
        raise FileNotFoundError(f"File {full_file_path} does not exist. Please check if file exists "
                                f"and ensure that filename case matches exactly.")

    file_extension = pathlib.Path(full_file_path).suffix.lower()

    if file_extension == '.csv':
        try:
            input_data_frame = pd.read_csv(full_file_path, dtype='str')
        except IOError:
            print(f"There was an error opening file {full_file_path}. It might be open in another program. "
                  f"Please close the program keeping the file open and try again.")
    elif file_extension == '.xls' or file_extension.lower() == '.xlsx':
        try:
            input_data_frame = pd.read_excel(full_file_path, dtype='str')
        except IOError:
            print(f"There was an error opening file {full_file_path}. It might be open in Excel. "
                  f"Please close Excel and try again.")
    else:
        raise FileNotFoundError(f"{file_extension} files are not currently supported. "
                                f"Please convert to CSV or Microsoft Excel")

    return input_data_frame
