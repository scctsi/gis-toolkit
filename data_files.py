import os
from data_structure import DataSource, RasterSource
from sedoh_data_structure import SedohDataSource


class DataFiles:
    def __init__(self):
        self.data_files = {}
        data_files_path = './data_files'
        for folder in os.listdir(data_files_path):
            data_source = ''
            for source in list(SedohDataSource):
                if folder in str(source).lower():
                    data_source = source
                    self.data_files.update({data_source: []})
                    break
            for file in os.listdir(f"{data_files_path}/{folder}"):
                if file.endswith('.csv'):
                    self.data_files[data_source].append(
                        DataSource()
                    )
                elif '.' not in file:
                    data_source = (data_source, file)
                    self.data_files.update({data_source: []})
                    for raster in os.listdir(f"{data_files_path}/{folder}/{file}"):
                        self.data_files[data_source].append(
                            RasterSource()
                        )