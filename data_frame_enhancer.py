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


def acs_dicts(data_elements):
    data_sets = {}
    data_sources = {}
    data_set_variables = {}
    for data_element in data_elements:
        data_set_name = value_getter.get_acs_dataset_name(data_element.source_variable)
        data_sources.update({data_element.source_variable: data_element})
        if data_set_name in data_sets.keys():
            data_sets[data_set_name] = data_sets[data_set_name] + ',' + data_element.source_variable
            data_set_variables[data_set_name].append(data_element.variable_name)
        else:
            data_sets.update({data_set_name: data_element.source_variable})
            data_set_variables.update({data_set_name: [data_element.variable_name]})
    return data_sets, data_sources, data_set_variables


def acs_data_sets_dict(data_elements):
    data_sets = {}
    for data_element in data_elements:
        data_set_name = value_getter.get_acs_dataset_name(data_element.source_variable)
        if data_set_name in data_sets.keys():
            data_sets[data_set_name] = data_sets[data_set_name] + ',' + data_element.source_variable
        else:
            data_sets.update({data_set_name: data_element.source_variable})
    return data_sets


def acs_data_sources_dict(data_elements):
    data_sources = {}
    for data_element in data_elements:
        data_sources.update({data_element.source_variable: data_element})
    return data_sources


def acs_data_set_variables_dict(data_elements):
    data_set_variables = {}
    for data_element in data_elements:
        data_set_name = value_getter.get_acs_dataset_name(data_element.source_variable)
        if data_set_name in data_set_variables.keys():
            data_set_variables[data_set_name].append(data_element.variable_name)
        else:
            data_set_variables.update({data_set_name: [data_element.variable_name]})
    return data_set_variables


class DataFrameEnhancer:
    def __init__(self, data_frame, data_elements, data_files, data_key):
        self.data_frame = data_frame
        self.data_elements = data_elements
        self.data_files = data_files
        self.data_key = data_key
        self.geoenhanced_cache = GeoenhancedCache()

    def save_enhancement_progress(self, index, status="Incomplete", error_message="", data_element_on_error=""):
        check_save_file()
        with open('temp/enhancer_save_file.json', "r+") as save_file:
            data = json.load(save_file)
            if self.data_key not in data.keys():
                data[self.data_key] = {'last_successful_line': index, "status": "Incomplete", "error_message": "", "data_element_on_error": ""}
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
            if self.data_key in data.keys() and data[self.data_key]['status'] == "Incomplete":
                index = data[self.data_key]['last_successful_line']
            else:
                index = 0
                self.save_enhancement_progress(index)
        return index

    def add_data_elements(self):
        for data_element in self.data_elements:
            self.data_frame[data_element.variable_name] = ""

    def arrange_data_elements(self):
        acs_data_elements = []
        non_acs_data_elements = []
        for data_element in self.data_elements:
            if data_element.data_source == sds.SedohDataSource.ACS:
                acs_data_elements.append(data_element)
            else:
                non_acs_data_elements.append(data_element)
        return acs_data_elements, non_acs_data_elements

    def get_data_element_values(self):
        progress = self.load_enhancement_progress()
        enhanced_file_path = './temp/enhanced_' + self.data_key + '2.csv'
        self.geoenhanced_cache.load_cache(enhanced_file_path)
        acs_data_elements, non_acs_data_elements = self.arrange_data_elements()
        # data_sets, data_sources, data_set_variables = acs_dicts(acs_data_elements)
        data_sets = acs_data_sets_dict(acs_data_elements)
        data_sources = acs_data_sources_dict(acs_data_elements)
        data_set_variables = acs_data_set_variables_dict(acs_data_elements)
        for index, row in self.data_frame.iloc[progress:].iterrows():
            progress_bar.progress(index, len(self.data_frame.index), "Enhancing with SEDoH data elements")
            arguments = {"fips_concatenated_code": self.data_frame.iloc[index][constant.GEO_ID_NAME]}
            if self.geoenhanced_cache.in_cache(arguments['fips_concatenated_code']):
                for data_element in self.data_elements:
                    self.data_frame.iloc[index][data_element.variable_name] = \
                        self.geoenhanced_cache.get_value_from_cache(arguments['fips_concatenated_code'], data_element)
            # NEEDS UPDATE AFTER MERGED WITH MAIN!!!
            elif not arguments['fips_concatenated_code'] == "Placeholder_Not_Found":
                for data_set in data_sets:
                    try:
                        values_dict = value_getter.get_acs_values(data_set, data_sets[data_set], arguments)
                    except requests.exceptions.RequestException as e:
                        self.save_enhancement_progress(index, "Incomplete", str(e))
                        raise SystemExit(e)
                    if values_dict == constant.NOT_AVAILABLE:
                        for variable in data_set_variables[data_set]:
                            self.data_frame.iloc[index][variable] = constant.NOT_AVAILABLE
                    else:
                        # values_dict = {source_variable: value}, data_sources = {source_variable: data_element}
                        for source in values_dict:
                            if data_sources[source].get_strategy == GetStrategy.CALCULATION:
                                self.data_frame.iloc[index][data_sources[source].variable_name] = \
                                    value_getter.get_acs_calculation(data_sources[source].variable_name, values_dict[source], arguments, self.data_files)
                            else:
                                self.data_frame.iloc[index][data_sources[source].variable_name] = values_dict[source]
                for data_element in non_acs_data_elements:
                    self.data_frame.iloc[index][data_element.variable_name] = \
                            value_getter.get_value(data_element, arguments, self.data_files)
            if index == 0:
                self.data_frame.iloc[[index]].to_csv(enhanced_file_path, index=False)
            else:
                self.data_frame.iloc[[index]].to_csv(enhanced_file_path, index=False, header=False, mode='a')
            self.geoenhanced_cache.set_cache(self.data_frame.iloc[index][constant.GEO_ID_NAME], self.data_frame.iloc[[index]])
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