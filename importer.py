import os
import pathlib
import pandas as pd
import rasterio
import constant
from dotenv import load_dotenv

load_dotenv()

format_key = {"year": constant.YEAR_FIRST, "month": constant.MONTH_FIRST, "day": constant.DAY_FIRST}


def import_file(full_file_path, version='latest'):
    input_data_frame = None

    if not (os.path.isfile(full_file_path)):
        raise FileNotFoundError(f"File {full_file_path} does not exist. Please check if file exists in input folder "
                                f"and ensure that filename case matches exactly.")

    file_extension = pathlib.Path(full_file_path).suffix.lower()

    if file_extension == '.csv':
        try:
            input_data_frame = pd.read_csv(full_file_path, dtype=str)
        except IOError:
            print(f"There was an error opening file {full_file_path}. It might be open in another program. "
                  f"Please close the program keeping the file open and try again.")
    elif file_extension == '.xls' or file_extension == '.xlsx':
        try:
            input_data_frame = pd.read_excel(full_file_path, dtype=str)
        except IOError:
            print(f"There was an error opening file {full_file_path}. It might be open in Excel. "
                  f"Please close Excel and try again.")
    elif file_extension == '.txt':  # NOTE: Support is included only for tab delimited text files
        input_data_frame = pd.read_csv(full_file_path, sep='\t', lineterminator='\r', dtype=str)
    elif file_extension == '.tif':
        input_data_frame = rasterio.open(full_file_path)
    else:
        raise FileNotFoundError(f"{file_extension} files are not currently supported. "
                                f"Please convert to CSV or Microsoft Excel")
    if version == 'comprehensive':
        try:
            date_format = format_key[os.getenv('date_format')]
        except KeyError:
            date_format = None
        print(date_format)
        try:
            input_data_frame[constant.ADDRESS_START_DATE] = pd.to_datetime(
                input_data_frame[constant.ADDRESS_START_DATE], format=date_format, infer_datetime_format=True)
            input_data_frame[constant.ADDRESS_END_DATE] = pd.to_datetime(
                input_data_frame[constant.ADDRESS_END_DATE], format=date_format, infer_datetime_format=True)
        except KeyError:
            raise Exception(f"{full_file_path} is missing 'address_start_date' and/or 'address_end_date' columns.")
    return input_data_frame
