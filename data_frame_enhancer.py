import pandas as pd
from data_structure import GetStrategy
import constant
import sedoh_data_structure as sds
import value_getter
import progress_bar
import os
import json
import importer
import requests


def check_temp_dir():
    if not os.path.isdir('./temp'):
        os.mkdir('./temp')
    return None


def check_save_file():
    check_temp_dir()
    if not os.path.exists('temp/enhancer_save_file.json'):
        with open('temp/enhancer_save_file.json', "w") as save_file:
            json.dump({}, save_file)
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
            state_codes += "0" + str(i) + ","
        else:
            state_codes += str(i) + ","
    return "for=tract:*&in=county:*&in=state:" + state_codes[:-1:]


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

    def data_sources(self):
        """
        :return: {data element source variable: data element}
        """
        data_sources = map(lambda data_element: (data_element.source_variable, data_element), self.acs_elements)
        return dict(data_sources)

    def data_set_variables(self):
        """
        :return: {acs data set: [data element variable names]}
        """
        data_set_variables = {}
        for data_element in self.acs_elements:
            data_set_name = value_getter.get_acs_dataset_name(data_element.source_variable)
            if data_set_name in data_set_variables.keys():
                data_set_variables[data_set_name].append(data_element.variable_name)
            else:
                data_set_variables.update({data_set_name: [data_element.variable_name]})
        return data_set_variables

    def data_set_elements(self):
        data_set_elements = {}
        for data_element in self.acs_elements:
            data_set_name = value_getter.get_acs_dataset_name(data_element.source_variable)
            if data_set_name in data_set_elements.keys():
                data_set_elements[data_set_name].append(data_element)
            else:
                data_set_elements.update({data_set_name: [data_element]})
        return data_set_elements

    def data_frames(self):
        data_sets = self.data_sets()
        geography = get_geography()
        data_frames = map(lambda data_set: value_getter.get_acs_batch(data_set, data_sets[data_set], geography), data_sets)
        return list(data_frames)

    def retrieve(self):
        return self.data_sets(), self.data_sources(), self.data_set_variables()


class DataFrameEnhancer:
    def __init__(self, data_frame, data_elements, data_files, data_key, test_mode=False):
        self.data_frame = data_frame
        self.data_elements = data_elements
        self.data_files = data_files
        self.data_key = data_key
        self.test_mode = test_mode
        self.geoenhanced_cache = GeoenhancedCache()
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

    def save_enhancement_progress(self, index, status="Incomplete", error_message="", data_element_on_error=""):
        check_save_file()
        with open('temp/enhancer_save_file.json', "r+") as save_file:
            data = json.load(save_file)
            if self.data_key not in data.keys():
                data[self.data_key] = {'last_successful_line': index, "status": "Incomplete", "error_message": "",
                                       "data_element_on_error": ""}
            else:
                data[self.data_key]['last_successful_line'] = index
                data[self.data_key]['status'] = status
                data[self.data_key]['error_message'] = error_message
                data[self.data_key]['data_element_on_error'] = data_element_on_error
            save_file.seek(0)
            json.dump(data, save_file, indent=4)
            save_file.truncate()

    def load_enhancement_progress(self):
        check_save_file()
        with open('temp/enhancer_save_file.json') as save_file:
            data = json.load(save_file)
            if self.data_key in data.keys():
                index = data[self.data_key]['last_successful_line']
            else:
                index = 0
                self.save_enhancement_progress(index)
        return index

    def load_enhancement_status(self):
        check_save_file()
        with open('temp/enhancer_save_file.json') as save_file:
            data = json.load(save_file)
        if self.data_key in data.keys() and data[self.data_key]['status'] == 'Complete':
            file_name, extension = data_key_to_file_name(self.data_key)
            print(file_name + "." + extension + " has already been enhanced.")
            print("Please look at output/" + file_name + "_enhanced." + extension + " for enhanced data.")
            print(
                "If you would like to enhance a new data set, please make sure to use a new and unique file name (different from " + file_name + "." + extension + ")")

    def add_data_elements(self):
        for data_element in self.data_elements:
            self.data_frame[data_element.variable_name] = ""

    def get_data_element_values(self):
        progress = self.load_enhancement_progress()
        self.load_enhancement_status()
        enhanced_file_path = './temp/enhanced_' + self.data_key + '.csv'
        self.geoenhanced_cache.load_cache(enhanced_file_path)
        data_frames = self.acs_data_source.data_frames()
        data_set_elements = self.acs_data_source.data_set_elements()
        # data_sets, data_sources, data_set_variables = self.acs_data_source.retrieve()
        # calculation_sources = ','.join(list(filter(lambda x: ',' in x, list(data_sources.keys()))))
        for index, row in self.data_frame.iloc[progress:].iterrows():
            progress_bar.progress(index, len(self.data_frame.index), "Enhancing with SEDoH data elements")
            arguments = {"fips_concatenated_code": self.data_frame.iloc[index][constant.GEO_ID_NAME]}
            geo_id = arguments["fips_concatenated_code"]
            if self.geoenhanced_cache.in_cache(arguments["fips_concatenated_code"]):
                for data_element in self.data_elements:
                    self.data_frame.iloc[index][data_element.variable_name] = \
                        self.geoenhanced_cache.get_value_from_cache(geo_id, data_element)
            elif not geo_id == constant.ADDRESS_NOT_GEOCODABLE:
                for data_set in data_frames:
                    for data_element in data_set_elements[data_set]:
                        state_code = geo_id[0:2]
                        county_code = geo_id[2:5]
                        tract_code = geo_id[5:11]
                        # TODO: Find index based on codes
                        idx = 1
                        # if source in calculation_sources:
                        #     if source == 'S1701_C01_042E':
                        #         self.data_frame.iloc[index]['percent_below_200_of_fed_poverty_level'] = \
                        #             value_getter.get_acs_calculation('percent_below_200_of_fed_poverty_level',
                        #                                              [values_dict[source],
                        #                                               values_dict['S1701_C01_001E']], arguments,
                        #                                              self.data_files)
                        #     if source == 'S1701_C01_043E':
                        #         self.data_frame.iloc[index]['percent_below_300_of_fed_poverty_level'] = \
                        #             value_getter.get_acs_calculation('percent_below_200_of_fed_poverty_level',
                        #                                              [values_dict[source],
                        #                                               values_dict['S1701_C01_001E']], arguments,
                        #                                              self.data_files)
                        value = data_frames[data_set].iloc[idx][data_element.source_variable]
                        if data_element.get_strategy == GetStrategy.CALCULATION:
                            self.data_frame.iloc[index][data_element.variable_name] = \
                                value_getter.get_acs_calculation(data_element.variable_name,
                                                                 value, arguments, self.data_files)
                        else:
                            self.data_frame.iloc[index][data_element.variable_name] = value
                for data_element in self.non_acs_data_elements:
                    self.data_frame.iloc[index][data_element.variable_name] = \
                        value_getter.get_value(data_element, arguments, self.data_files)
                self.geoenhanced_cache.set_cache(arguments["fips_concatenated_code"], self.data_frame.iloc[[index]])
            if index == 0:
                self.data_frame.iloc[[index]].to_csv(enhanced_file_path, index=False)
            else:
                self.data_frame.iloc[[index]].to_csv(enhanced_file_path, index=False, header=False, mode='a')
            self.save_enhancement_progress(index + 1)
        self.save_enhancement_progress(self.load_enhancement_progress(), "Complete")
        self.data_frame = importer.import_file(enhanced_file_path)

    def enhance(self):
        self.add_data_elements()
        self.get_data_element_values()
        return self.data_frame


class GeoenhancedCache:
    def __init__(self):
        self.data_frame = pd.DataFrame(columns=[constant.GEO_ID_NAME])

    def load_cache(self, file_path):
        if os.path.exists(file_path):
            self.data_frame = importer.import_file(file_path)
            self.data_frame.drop_duplicates(subset=[constant.GEO_ID_NAME], inplace=True, ignore_index=True)

    def in_cache(self, geo_id):
        if geo_id in self.data_frame[constant.GEO_ID_NAME].values:
            return True
        else:
            return False

    def set_cache(self, geo_id, input_row):
        if not self.in_cache(geo_id):
            self.data_frame = pd.concat([self.data_frame, input_row], ignore_index=True)

    def get_value_from_cache(self, geo_id, data_element):
        index = self.data_frame.index[self.data_frame[constant.GEO_ID_NAME] == geo_id][0]
        return self.data_frame.iloc[index][data_element.variable_name]
