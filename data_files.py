import os
from datetime import datetime

from data_structure import ACSSource, DataSource, RasterSource, NasaSource
from sedoh_data_structure import SedohDataSource
import json


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


class DataFiles:
    def __init__(self):
        with open('temp/geocoder_save_file.json') as save_file:
            data = json.load(save_file)
        self.data_files = {}
        for source in data:
            if data[source]["type"] == "ACS":
                data_sources = []
                for version in data[source]["versions"]:
                    data_sources.append(ACSSource(
                        acs_year=version["acs_year"],
                        start_date=datetime.strptime(version["valid_start_date"], "%Y-%m-%dT%H:%M:%S.%fZ"),
                        end_date=datetime.strptime(version["valid_end_date"], "%Y-%m-%dT%H:%M:%S.%fZ")
                    ))
                self.data_files.update({vars(SedohDataSource)["_member_map_"][source]: data_sources})
            elif data[source]["type"] == "Census":
                data_sources = []
                for version in data[source]["versions"]:
                    data_sources.append(DataSource(
                        file_name=version["file_path"],
                        tract_column=version["census_tract_column_name"],
                        start_date=datetime.strptime(version["valid_start_date"], "%Y-%m-%dT%H:%M:%S.%fZ"),
                        end_date=datetime.strptime(version["valid_end_date"], "%Y-%m-%dT%H:%M:%S.%fZ")
                    ))
                self.data_files.update({vars(SedohDataSource)["_member_map_"][source]: data_sources})
            elif data[source]["type"] == "Raster":
                pollutants = [key for key in data[source] if key != "type"]
                for pollutant in pollutants:
                    data_sources = []
                    for version in data[source][pollutant]["versions"]:
                        data_sources.append(RasterSource(
                            file_name=version["file_path"],
                            latitude_range=version["latitude_range"],
                            longitude_range=version["longitude_range"],
                            precision=version["precision"],
                            start_date=datetime.strptime(version["valid_start_date"], "%Y-%m-%dT%H:%M:%S.%fZ"),
                            end_date=datetime.strptime(version["valid_end_date"], "%Y-%m-%dT%H:%M:%S.%fZ")
                        ))
                    self.data_files.update({(vars(SedohDataSource)["_member_map_"][source], pollutant): data_sources})
            elif data[source]["type"] == "Nasa":
                pollutants = [key for key in data[source] if key != "type"]
                for pollutant in pollutants:
                    data_sources = []
                    for version in data[source][pollutant]["versions"]:
                        data_sources.append(NasaSource(
                            file_name=version["file_path"],
                            start_date=datetime.strptime(version["valid_start_date"], "%Y-%m-%dT%H:%M:%S.%fZ"),
                            end_date=datetime.strptime(version["valid_end_date"], "%Y-%m-%dT%H:%M:%S.%fZ")
                        ))
                    self.data_files.update({(vars(SedohDataSource)["_member_map_"][source], pollutant): data_sources})
        print("Data Files Load Complete")





