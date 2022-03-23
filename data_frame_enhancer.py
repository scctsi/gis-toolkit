import pandas as pd
import constant
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

    def get_data_element_values(self):
        progress = self.load_enhancement_progress()
        enhanced_file_path = './temp/enhanced_' + self.data_key + '.csv'
        self.geoenhanced_cache.load_cache(enhanced_file_path)
        for index, row in self.data_frame.iloc[progress:].iterrows():
            progress_bar.progress(index, len(self.data_frame.index), "Enhancing with SEDoH data elements")
            if not self.data_frame.iloc[index][constant.GEO_ID_NAME] == constant.ADDRESS_NOT_GEOCODABLE:
                arguments = {"fips_concatenated_code": self.data_frame.iloc[index][constant.GEO_ID_NAME]}
                for data_element in self.data_elements:
                    try:
                        self.data_frame.iloc[index][data_element.variable_name] = \
                            value_getter.get_value(data_element, arguments, self.data_files, self.geoenhanced_cache)
                    except requests.exceptions.RequestException as e:
                        self.save_enhancement_progress(index, "Incomplete", str(e), data_element.friendly_name)
                        raise SystemExit(e)
                self.geoenhanced_cache.set_cache(self.data_frame.iloc[index][constant.GEO_ID_NAME], self.data_frame.iloc[[index]])
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