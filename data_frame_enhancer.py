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
        for index, row in self.data_frame.iloc[progress:].iterrows():
            progress_bar.progress(index, len(self.data_frame.index), "Enhancing with SEDoH data elements")
            arguments = {"fips_concatenated_code": self.data_frame.iloc[index][constant.GEO_ID_NAME]}
            for data_element in self.data_elements:
                try:
                    self.data_frame.iloc[index][data_element.variable_name] = \
                        value_getter.get_value(data_element, arguments, self.data_files)
                except requests.exceptions.RequestException as e:
                    self.save_enhancement_progress(index, "Incomplete", str(e), data_element.friendly_name)
                    raise SystemExit(e)
            if index == 0:
                self.data_frame.iloc[[index]].to_csv('./temp/enhanced_' + self.data_key + '.csv', index=False)
            else:
                self.data_frame.iloc[[index]].to_csv('./temp/enhanced_' + self.data_key + '.csv', index=False, header=False, mode='a')
            self.save_enhancement_progress(index + 1)
        self.save_enhancement_progress(self.load_enhancement_progress(), "Complete")
        self.data_frame = importer.import_file('./temp/enhanced_' + self.data_key + '.csv')

    def enhance(self):
        self.add_data_elements()
        self.get_data_element_values()
        return self.data_frame

