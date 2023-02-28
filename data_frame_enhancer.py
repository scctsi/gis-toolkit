import json
from datetime import datetime
import pandas as pd
import constant
import sedoh_data_structure as sds
import value_getter
import os
import importer
import requests
from data_structure import GetStrategy


def check_temp_dir():
    if not os.path.isdir('./temp'):
        os.mkdir('./temp')
    return None


def check_acs_cache_dir():
    if not os.path.isdir('./acs_cache'):
        os.mkdir('./acs_cache')
        os.mkdir('./acs_cache/latest')
        os.mkdir('./acs_cache/comprehensive')
    return None


def data_key_to_file_name(data_key):
    index = data_key.rindex('_')
    file_name = data_key[:index]
    extension = data_key[index + 1:]
    return file_name, extension


def get_geography():
    state_codes = ""
    for i in range(1, 57):
        if i < 10:
            state_codes += f"0{str(i)},"
        else:
            state_codes += f"{str(i)},"
    return f"for=tract:*&in=county:*&in=state:{state_codes[:-1:]}"


def write_xlsxwriter_output(excel_path, data_frames):
    writer = pd.ExcelWriter(excel_path, engine='xlsxwriter')
    for data_element in data_frames:
        data_frames[data_element].to_excel(writer, sheet_name=data_element.sheet_name)
    writer.save()


def normalize_data_frame(data_frame):
    data_frame = data_frame.loc[:, ~data_frame.columns.duplicated()]
    data_frame.index = data_frame[constant.GEO_ID_NAME]
    return data_frame


class ACSCache:
    def __init__(self, acs_data_source, acs_sources, version, test_mode):
        self.days = 30
        self.acs_data_source = acs_data_source
        self.version = version
        self.test_mode = test_mode
        self.acs_sources = acs_sources
        self.sets = {'subject': 'acs/acs5/subject', 'profile': 'acs/acs5/profile', 'default': 'acs/acs5'}
        self.files = {'acs/acs5/subject': 'subject', 'acs/acs5/profile': 'profile', 'acs/acs5': 'default'}

    def check_cache(self):
        check_acs_cache_dir()
        file_path = f'./acs_cache/{self.version}/timestamp.csv'
        if os.path.exists(file_path):
            return self.check_date(file_path)
        else:
            return False

    def check_date(self, file_path):
        time_frame = importer.import_file(file_path)
        time_frame[constant.DATE_COLUMN] = pd.to_datetime(time_frame[constant.DATE_COLUMN], infer_datetime_format=True)
        if (datetime.today() - time_frame.loc[0, constant.DATE_COLUMN]).days <= self.days:
            return True
        else:
            return False

    def load_single(self):
        if self.check_cache():
            data_frames = {}
            for file in os.listdir(f'./acs_cache/{self.version}'):
                name = file[:file.index('.')]
                if name in self.sets.keys():
                    data_frame = normalize_data_frame(importer.import_file(f'./acs_cache/latest/{file}'))
                    data_frames.update({self.sets[name]: data_frame})
        else:
            data_frames = self.acs_data_source.data_frames(test_mode=self.test_mode)
            self.set_single(data_frames)
        return data_frames

    def set_single(self, data_frames):
        pd.DataFrame(data={constant.DATE_COLUMN: [datetime.today()]}).to_csv(f'./acs_cache/latest/timestamp.csv')
        for data_set in data_frames:
            data_frames[data_set].to_csv(f'./acs_cache/latest/{self.files[data_set]}.csv')

    def load_comprehensive(self):
        if self.check_cache():
            comprehensive_frames = {}
            for file in os.listdir(f'./acs_cache/{self.version}'):
                if '_' in file:
                    year = file[:file.index('_')]
                    name = file[file.index('_') + 1: file.index('.')]
                    if year not in comprehensive_frames.keys():
                        comprehensive_frames.update({year: {}})
                    data_frame = normalize_data_frame(importer.import_file(f'./acs_cache/comprehensive/{file}'))
                    comprehensive_frames[year].update({self.sets[name]: data_frame})
        else:
            comprehensive_frames = self.acs_data_source.comprehensive_data_frames(self.acs_sources, self.test_mode)
            self.set_comprehensive(comprehensive_frames)
        return comprehensive_frames

    def set_comprehensive(self, comprehensive_frames):
        pd.DataFrame(data={constant.DATE_COLUMN: [datetime.today()]}).to_csv(f'./acs_cache/comprehensive/timestamp.csv')
        for year in comprehensive_frames:
            for data_set in comprehensive_frames[year]:
                comprehensive_frames[year][data_set].to_csv(f'./acs_cache/comprehensive/{year}_{self.files[data_set]}.csv')


class ACSDataSource:
    def __init__(self, acs_elements):
        self.acs_elements = acs_elements
        # self.omit_source_string = {"2012": ["S2801_C02_011E", "S2801_C02_019E", "S2201_C04_001E", "S1602_C04_001E"],
        #                            "2013": ["S2801_C02_011E", "S2801_C02_019E", "S2201_C04_001E", "S1602_C04_001E"],
        #                            "2014": ["S2801_C02_011E", "S2801_C02_019E", "S2201_C04_001E", "S1602_C04_001E"],
        #                            "2015": ["S2801_C02_011E", "S2801_C02_019E"],
        #                            "2016": ["S2801_C02_011E", "S2801_C02_019E"]}
        with open("source_key.json", "r+") as source_key_file:
            self.source_key = json.load(source_key_file)

    def data_sets(self, data_year):
        """
        :return: {acs data set: joined source variable string}
        """
        data_sets = {}
        for data_element in self.acs_elements:
            source_variable = self.source_key[data_element.variable_name][data_year]
            # if data_year not in self.omit_source_string.keys() or source_variable not in \
            #         self.omit_source_string[data_year]:
            if source_variable != "":
                data_set_name = value_getter.get_acs_dataset_name(source_variable)
                if data_set_name in data_sets.keys():
                    data_sets[data_set_name] = data_sets[data_set_name] + ',' + source_variable
                else:
                    data_sets.update({data_set_name: source_variable})
        return data_sets

    def data_element_data_set(self):
        """
        :return: {data element: data set}
        """
        data_element_data_set = map(
            lambda data_element: (data_element, value_getter.get_acs_dataset_name(data_element.source_variable)),
            self.acs_elements)
        return dict(data_element_data_set)

    def data_frames(self, data_year="2018", test_mode=False):
        """
        :return: {acs data set: data frame of data set data from all acs census tracts}
        """
        data_sets = self.data_sets(data_year)
        geography = get_geography()
        try:
            data_frames = map(lambda data_set: (
                data_set, value_getter.get_acs_batch(data_set, data_sets[data_set], geography, data_year=data_year,
                                                     test_mode=test_mode)), data_sets)
            return dict(data_frames)
        except requests.exceptions.RequestException as e:
            SystemExit(e)

    def comprehensive_data_frames(self, acs_sources, test_mode=False):
        """
        :return: {year: {acs data set: data frame of data set data from all acs census tracts}
        """
        comprehensive_data_frames = map(lambda acs_source: (
            acs_source.acs_year, self.data_frames(data_year=acs_source.acs_year, test_mode=test_mode)), acs_sources)
        return dict(comprehensive_data_frames)


class DataFrameEnhancer:
    def __init__(self, data_frame, data_elements, data_files, data_key, version, test_mode=False):
        self.data_frame = data_frame
        self.data_elements = data_elements
        self.data_files = data_files
        self.data_key = data_key
        self.version = version
        self.test_mode = test_mode
        self.non_raster_elements = self.group_raster_elements()
        self.acs_data_elements, self.non_acs_data_elements = self.group_acs_elements()
        self.acs_data_source = ACSDataSource(self.acs_data_elements)
        self.acs_cache = ACSCache(self.acs_data_source, self.data_files[sds.SedohDataSource.ACS], self.version, self.test_mode)
        self.LatLon = False
        if constant.LATITUDE in self.data_frame.columns and constant.LONGITUDE in self.data_frame.columns:
            self.LatLon = True

    def group_raster_elements(self):
        non_raster_data_elements = []
        for data_element in self.data_elements:
            if data_element.get_strategy != GetStrategy.RASTER_FILE:
                non_raster_data_elements.append(data_element)
        return non_raster_data_elements

    def group_acs_elements(self):
        acs_data_elements = []
        non_acs_data_elements = []
        for data_element in self.data_elements:
            if data_element.data_source == sds.SedohDataSource.ACS:
                acs_data_elements.append(data_element)
            else:
                non_acs_data_elements.append(data_element)
        return acs_data_elements, non_acs_data_elements

    def load_enhancement_job(self):
        check_temp_dir()
        if self.version == 'latest':
            if os.path.exists(f"./temp/enhanced_{self.data_key}.csv"):
                self.data_frame = importer.import_file(f"./temp/enhanced_{self.data_key}.csv")
                self.print_previous_enhancement()
            else:
                self.enhancement()
        elif self.version == 'comprehensive':
            file_name, extension = data_key_to_file_name(self.data_key)
            if os.path.exists(f"./output/comprehensive_enhanced_{file_name}.xlsx"):
                self.print_previous_enhancement()
            else:
                self.comprehensive_enhancement()

    def print_previous_enhancement(self):
        file_name, extension = data_key_to_file_name(self.data_key)
        print(f"{file_name}.{extension} has already been enhanced with {self.version} version of enhancement.")
        if self.version == 'latest':
            print(f"Please look at output/{file_name}_enhanced.{extension} for enhanced data.")
        if self.version == 'comprehensive':
            print(f"Please look at output/comprehensive_enhanced_{file_name}.xlsx for enhanced data.")
        print(
            f"If you would like to enhance a new data set, please make sure to use a new and unique file name (different from {file_name}.{extension})")

    def enhancement(self):
        acs_data_frames = self.acs_cache.load_single()
        acs_data_sets = self.acs_data_source.data_element_data_set()
        for data_element in self.data_elements:
            if data_element in self.non_raster_elements:
                if data_element in self.acs_data_elements:
                    enhancer_data_frame = acs_data_frames[acs_data_sets[data_element]]
                else:
                    enhancer_data_frame = value_getter.get_enhancer_data_frame(self.data_files[data_element.data_source][-1])
                self.data_frame = value_getter.enhance_data_element(
                    self.data_frame, enhancer_data_frame, data_element, self.data_files, self.version)
            elif self.LatLon:
                self.data_frame = value_getter.enhance_raster_element(
                    self.data_frame, data_element, self.data_files[data_element.data_source][-1])
        self.data_frame.to_csv(f"./temp/enhanced_{self.data_key}.csv")

    def comprehensive_enhancement(self):
        comprehensive_data_frames = self.acs_cache.load_comprehensive()
        data_sets = self.acs_data_source.data_element_data_set()
        file_name, extension = data_key_to_file_name(self.data_key)
        excel_path = f'./output/comprehensive_enhanced_{file_name}.xlsx'
        data_source_addresses = {}
        element_data_frames_output = {}
        for data_element in self.data_elements:
            element_data_frames = []
            if data_element.data_source not in data_source_addresses.keys():
                data_source_addresses.update({data_element.data_source: value_getter.organize_data_frame_by_source(
                    self.data_frame.copy(), self.data_files[data_element.data_source])})
            for data_source in self.data_files[data_element.data_source]:
                organized_data_frame = data_source_addresses[data_element.data_source][data_source.start_date]
                if len(organized_data_frame) > 0:
                    if data_element in self.non_raster_elements:
                        if data_element in self.acs_data_elements:
                            enhancer_data_frame = comprehensive_data_frames[data_source.acs_year][data_sets[data_element]]
                        else:
                            enhancer_data_frame = value_getter.get_enhancer_data_frame(data_source)
                        element_data_frames.append(value_getter.enhance_data_element(
                            organized_data_frame.copy(), enhancer_data_frame, data_element, self.data_files, self.version))
                    elif self.LatLon:
                        element_data_frames.append(value_getter.enhance_raster_element(
                            organized_data_frame, data_element, data_source))
            if len(element_data_frames) > 0:
                element_data_frame = pd.concat(element_data_frames, ignore_index=True)
                element_data_frame.reset_index(drop=True, inplace=True)
                element_data_frames_output.update({data_element: element_data_frame})
        write_xlsxwriter_output(excel_path, element_data_frames_output)

    def enhance(self):
        self.load_enhancement_job()
        if self.version == 'latest':
            return self.data_frame

