import os.path
from enum import Enum
import pandas as pd
import importer


class GetStrategy(Enum):
    PUBLIC_API = 1
    PRIVATE_API = 2
    CALCULATION = 3
    FILE = 4
    FILE_AND_CALCULATION = 5
    RASTER_FILE = 6


class DataElement:
    def __init__(self, data_source, friendly_name, variable_name, source_variable, get_strategy, sheet_name):
        self.data_source = data_source         # External data source
        self.friendly_name = friendly_name     # A friendly name for the data element
        self.variable_name = variable_name     # A variable name that can be used by most statistical software
        self.source_variable = source_variable # The name of the variable at the data source
        self.get_strategy = get_strategy       # How do we acquire the value of this variable?
        self.sheet_name = sheet_name           # Title of Excel sheet used in v2.0


class DataSource:
    def __init__(self, file_name, tract_column, start_date, end_date, directory="data_files"):
        if os.path.exists(f"./{directory}/{file_name}"):
            self.data_frame = importer.import_file(f'./{directory}/{file_name}')
        else:
            self.data_frame = pd.DataFrame()
        self.file_name = file_name
        self.tract_column = tract_column
        self.start_date = start_date
        self.end_date = end_date


class RasterSource:
    def __init__(self, file_name, latitude_range, longitude_range, precision, start_date, end_date, directory="data_files"):
        self.file_name = file_name
        if os.path.exists(f"./{directory}/{file_name}"):
            raster_data = importer.import_file(f'./{directory}/{file_name}')
            self.array = raster_data.read(1)
            raster_data.close()
            self.latitude_range = latitude_range
            self.longitude_range = longitude_range
            self.precision = precision
            self.step = round((self.latitude_range[1] - self.latitude_range[0]) / self.array.shape[0], self.precision)
            self.latitude_transform = self.array.shape[0] - 1
            self.start_date = start_date
            self.end_date = end_date


class NasaSource:
    def __init__(self, file_name, start_date, end_date, directory='data_files'):
        self.file_name = file_name
        self.start_date = start_date
        self.end_date = end_date
        self.directory = directory

    def read(self):
        if os.path.exists(f'./{self.directory}/{self.file_name}'):
            self.raster_data = importer.import_file(f'./data_files/{self.file_name}')
        else:
            raise Exception("Enhancing Nasa variable for which there is no data file.")
        return self.raster_data

    def close(self):
        self.raster_data.close()


class ACSSource:
    def __init__(self, acs_year, start_date, end_date):
        self.acs_year = acs_year
        self.start_date = start_date
        self.end_date = end_date
