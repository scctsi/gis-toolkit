import os
import pathlib
import pandas as pd


def export_file(data_frame, full_file_path):
    file_extension = pathlib.Path(full_file_path).suffix.lower()

    if not (file_extension == '.csv' or file_extension == '.xls' or file_extension == '.xlsx'):
        raise Exception(f"File extension {file_extension} is not currently supported. "
                        f"The supported extensions are .csv, .xls, and .xlsx")

    if file_extension == '.csv':
        data_frame.to_csv(full_file_path)
    elif file_extension.lower() == '.xls' or file_extension.lower() == '.xlsx':
        data_frame.to_excel(full_file_path, sheet_name='Export')

    return None
