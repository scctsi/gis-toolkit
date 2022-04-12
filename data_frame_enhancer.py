from datetime import datetime
import pandas as pd
from data_structure import GetStrategy
import constant
import sedoh_data_structure as sds
import value_getter
import progress_bar
import os
import importer
import requests


def check_temp_dir():
    if not os.path.isdir('./temp'):
        os.mkdir('./temp')
    return None


def check_cache_dir():
    if not os.path.isdir('./cache'):
        os.mkdir('./cache')
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


class ACSDataSource:
    def __init__(self, acs_elements):
        self.acs_elements = acs_elements

    def data_sets(self):
        """
        :return: {acs data set: joined source variable string}
        """
        data_sets = {}
        for data_element in self.acs_elements:
            data_set_name = value_getter.get_acs_dataset_name(data_element.source_variable)
            if data_set_name in data_sets.keys():
                data_sets[data_set_name] = data_sets[data_set_name] + ',' + data_element.source_variable
            else:
                data_sets.update({data_set_name: data_element.source_variable})
        return data_sets

    def data_set_elements(self):
        """
        :return: {acs data set: list of corresponding data elements}
        """
        data_set_elements = {}
        for data_element in self.acs_elements:
            data_set_name = value_getter.get_acs_dataset_name(data_element.source_variable)
            if data_set_name in data_set_elements.keys():
                data_set_elements[data_set_name].append(data_element)
            else:
                data_set_elements.update({data_set_name: [data_element]})
        return data_set_elements

    def data_frames(self, test_mode=False):
        """
        :return: {acs data set: data frame of data set data from all acs census tracts}
        """
        data_sets = self.data_sets()
        geography = get_geography()
        try:
            data_frames = map(lambda data_set: (
            data_set, value_getter.get_acs_batch(data_set, data_sets[data_set], geography, test_mode)), data_sets)
            return dict(data_frames)
        except requests.exceptions.RequestException as e:
            SystemExit(e)


class DataFrameEnhancer:
    def __init__(self, data_frame, data_elements, data_files, data_key, test_mode=False):
        self.data_frame = data_frame
        self.data_elements = data_elements
        self.data_files = data_files
        self.data_key = data_key
        self.test_mode = test_mode
        self.global_cache = GlobalCache()
        self.acs_data_elements, self.non_acs_data_elements = self.group_data_elements()
        self.acs_data_source = ACSDataSource(self.acs_data_elements)

    def group_data_elements(self):
        acs_data_elements = []
        non_acs_data_elements = []
        for data_element in self.data_elements:
            if data_element.data_source == sds.SedohDataSource.ACS:
                acs_data_elements.append(data_element)
            else:
                non_acs_data_elements.append(data_element)
        return acs_data_elements, non_acs_data_elements

    def add_data_elements(self):
        for data_element in self.data_elements:
            self.data_frame[data_element.variable_name] = ""

    def load_enhancement_job(self):
        check_temp_dir()
        if os.path.exists(f"./temp/enhanced_{self.data_key}.csv"):
            self.data_frame = importer.import_file(f"./temp/enhanced_{self.data_key}.csv")
            file_name, extension = data_key_to_file_name(self.data_key)
            print(f"{file_name}.{extension} has already been enhanced.")
            print(f"Please look at output/{file_name}_enhanced.{extension} for enhanced data.")
            print(f"If you would like to enhance a new data set, please make sure to use a new and unique file name (different from {file_name}.{extension})")
        else:
            self.get_data_element_values()

    def get_data_element_values(self):
        self.global_cache.load_cache()
        data_frames = self.acs_data_source.data_frames(self.test_mode)
        data_set_elements = self.acs_data_source.data_set_elements()
        for index, row in self.data_frame.iterrows():
            progress_bar.progress(index, len(self.data_frame.index), "Enhancing with SEDoH data elements")
            arguments = {"fips_concatenated_code": self.data_frame.iloc[index][constant.GEO_ID_NAME]}
            state_code = arguments["fips_concatenated_code"][0:2]
            county_code = arguments["fips_concatenated_code"][2:5]
            tract_code = arguments["fips_concatenated_code"][5:11]
            if self.global_cache.in_cache(arguments["fips_concatenated_code"]):
                cache_row = self.global_cache.get_cache_row(arguments["fips_concatenated_code"])
                for data_element in self.data_elements:
                    self.data_frame.iloc[index][data_element.variable_name] = cache_row.iloc[0][data_element.variable_name]
            elif not arguments["fips_concatenated_code"] == constant.ADDRESS_NOT_GEOCODABLE:
                for data_set in data_frames:
                    idx = data_frames[data_set].index[(data_frames[data_set]['state'] == state_code) & (
                                data_frames[data_set]['county'] == county_code) & (data_frames[data_set][
                                                                                       'tract'] == tract_code)].tolist()
                    for data_element in data_set_elements[data_set]:
                        if len(idx) == 0:
                            self.data_frame.iloc[index][data_element.variable_name] = constant.NOT_AVAILABLE
                        elif data_element.get_strategy == GetStrategy.CALCULATION:
                            if "," in data_element.source_variable:
                                source_var = data_element.source_variable[:data_element.source_variable.index(',')]
                                calc_var = data_element.source_variable[data_element.source_variable.index(',') + 1:]
                                self.data_frame.iloc[index][data_element.variable_name] = \
                                    value_getter.get_acs_calculation(data_element.variable_name,
                                                                     [data_frames[data_set].iloc[idx[0]][source_var],
                                                                      data_frames[data_set].iloc[idx[0]][calc_var]],
                                                                     arguments, self.data_files)
                            else:
                                self.data_frame.iloc[index][data_element.variable_name] = \
                                    value_getter.get_acs_calculation(data_element.variable_name,
                                                                     data_frames[data_set].iloc[idx[0]][
                                                                         data_element.source_variable], arguments, self.data_files)
                        else:
                            self.data_frame.iloc[index][data_element.variable_name] = \
                                data_frames[data_set].iloc[idx[0]][data_element.source_variable]
                for data_element in self.non_acs_data_elements:
                    self.data_frame.iloc[index][data_element.variable_name] = \
                        value_getter.get_value(data_element, arguments, self.data_files)
                self.global_cache.set_cache(arguments["fips_concatenated_code"], self.data_frame.iloc[[index]])
        self.global_cache.write_to_cache()
        self.data_frame.to_csv(f"./temp/enhanced_{self.data_key}.csv")

    def enhance(self):
        self.add_data_elements()
        self.load_enhancement_job()
        return self.data_frame


class GlobalCache:
    def __init__(self):
        self.data_frame = pd.DataFrame(columns=[constant.GEO_ID_NAME])
        self.timeframe = 7  # days

    def load_cache(self):
        check_cache_dir()
        if os.path.exists('./cache/global_cache.csv'):
            self.data_frame = pd.read_csv('./cache/global_cache.csv', parse_dates=[constant.DATE_COLUMN], infer_datetime_format=True)
            self.data_frame.drop(self.data_frame.index[(datetime.today() - self.data_frame[constant.DATE_COLUMN]).dt.days >= self.timeframe], inplace=True)

    def in_cache(self, geo_id):
        if geo_id in self.data_frame[constant.GEO_ID_NAME].values:
            return True
        else:
            return False

    def set_cache(self, geo_id, input_row):
        if not self.in_cache(geo_id):
            input_row.insert(len(input_row.columns), constant.DATE_COLUMN, [datetime.today()])
            self.data_frame = pd.concat([self.data_frame, input_row], ignore_index=True)

    def get_cache_row(self, geo_id):
        index = self.data_frame.index[self.data_frame[constant.GEO_ID_NAME] == geo_id][0]
        return self.data_frame.iloc[[index]]

    def write_to_cache(self):
        self.data_frame.to_csv('./cache/global_cache.csv')