from datetime import datetime
from datetime import timedelta
import pandas as pd
from data_structure import GetStrategy
import constant
import sedoh_data_structure as sds
import value_getter
import progress_bar
import os
import importer
import requests
from openpyxl import load_workbook


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


def source_intersection(source, row):
    if source.start_date <= row.iloc[0]['address_start_date'] <= source.end_date:
        return True
    else:
        return False


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
    def __init__(self, data_frame, data_elements, data_files, data_key, version=1, test_mode=False):
        self.data_frame = data_frame
        self.data_elements = data_elements
        self.data_files = data_files
        self.data_key = data_key
        self.version = version
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
        if self.version == 1:
            if os.path.exists(f"./temp/enhanced_{self.data_key}.csv"):
                self.data_frame = importer.import_file(f"./temp/enhanced_{self.data_key}.csv")
                self.print_previous_enhancement()
            else:
                self.add_data_elements()
                self.get_data_element_values()
        elif self.version == 2:
            file_name, extension = data_key_to_file_name(self.data_key)
            if os.path.exists(f"./output/comprehensive_enhanced_{file_name}.xlsx"):
                self.print_previous_enhancement()
            else:
                self.load_comprehensive_data_element_values()

    def print_previous_enhancement(self):
        file_name, extension = data_key_to_file_name(self.data_key)
        print(f"{file_name}.{extension} has already been enhanced with version {self.version}.")
        if self.version == 1:
            print(f"Please look at output/{file_name}_enhanced.{extension} for enhanced data.")
        if self.version == 2:
            print(f"Please look at output/comprehensive_enhanced_{file_name}.xlsx for enhanced data.")
        print(f"If you would like to enhance a new data set, please make sure to use a new and unique file name (different from {file_name}.{extension})")

    def get_data_element_values(self):
        self.global_cache.load_cache()
        data_frames = self.acs_data_source.data_frames(self.test_mode)
        data_set_elements = self.acs_data_source.data_set_elements()
        for index, row in self.data_frame.iterrows():
            progress_bar.progress(index, len(self.data_frame.index), "Enhancing with SEDoH data elements")
            arguments = {"fips_concatenated_code": self.data_frame.loc[index, constant.GEO_ID_NAME]}
            if self.global_cache.in_cache(arguments["fips_concatenated_code"]):
                cache_row = self.global_cache.get_cache_row(arguments["fips_concatenated_code"])
                for data_element in self.data_elements:
                    self.data_frame.loc[index, data_element.variable_name] = cache_row.loc[0, data_element.variable_name]
            elif not arguments["fips_concatenated_code"] == constant.ADDRESS_NOT_GEOCODABLE:
                for data_set in data_frames:
                    for data_element in data_set_elements[data_set]:
                        if arguments["fips_concatenated_code"] not in data_frames[data_set].index:
                            self.data_frame.loc[index, data_element.variable_name] = constant.NOT_AVAILABLE
                        elif data_element.get_strategy == GetStrategy.CALCULATION:
                            if "," in data_element.source_variable:
                                source_var = data_element.source_variable[:data_element.source_variable.index(',')]
                                calc_var = data_element.source_variable[data_element.source_variable.index(',') + 1:]
                                self.data_frame.loc[index, data_element.variable_name] = \
                                    value_getter.get_acs_calculation(data_element.variable_name,
                                                                     [data_frames[data_set].loc[arguments["fips_concatenated_code"], source_var],
                                                                      data_frames[data_set].loc[arguments["fips_concatenated_code"], calc_var]],
                                                                     arguments, self.data_files)
                            else:
                                self.data_frame.loc[index, data_element.variable_name] = \
                                    value_getter.get_acs_calculation(data_element.variable_name,
                                                                     data_frames[data_set].loc[arguments["fips_concatenated_code"],
                                                                         data_element.source_variable], arguments, self.data_files)
                        else:
                            self.data_frame.loc[index, data_element.variable_name] = \
                                data_frames[data_set].loc[arguments["fips_concatenated_code"], data_element.source_variable]
                for data_element in self.non_acs_data_elements:
                    self.data_frame.loc[index, data_element.variable_name] = \
                        value_getter.get_value(data_element, arguments, self.data_files)
                self.global_cache.set_cache(arguments["fips_concatenated_code"], self.data_frame.iloc[[index]])
        self.global_cache.write_to_cache()
        self.data_frame.to_csv(f"./temp/enhanced_{self.data_key}.csv")

    def load_comprehensive_data_element_values(self):
        check_temp_dir()
        data_frames = self.acs_data_source.data_frames(self.test_mode)
        data_set_elements = self.acs_data_source.data_set_elements()
        file_name, extension = data_key_to_file_name(self.data_key)
        excel_path = f'./output/comprehensive_enhanced_{file_name}.xlsx'
        for data_set in data_frames:
            for data_element in data_set_elements[data_set]:
                element_data_frame = self.data_frame.copy()
                element_data_frame[data_element.variable_name] = ''
                for index, row in element_data_frame.iterrows():
                    arguments = {"fips_concatenated_code": element_data_frame.loc[index, constant.GEO_ID_NAME]}
                    if not arguments["fips_concatenated_code"] == constant.ADDRESS_NOT_GEOCODABLE:
                        if arguments["fips_concatenated_code"] not in data_frames[data_set][constant.GEO_ID_NAME]:
                            element_data_frame.loc[index, data_element.variable_name] = constant.NOT_AVAILABLE
                        elif data_element.get_strategy == GetStrategy.CALCULATION:
                            if "," in data_element.source_variable:
                                source_var = data_element.source_variable[:data_element.source_variable.index(',')]
                                calc_var = data_element.source_variable[data_element.source_variable.index(',') + 1:]
                                element_data_frame.loc[index, data_element.variable_name] = \
                                    value_getter.get_acs_calculation(data_element.variable_name,
                                                                     [data_frames[data_set].loc[arguments["fips_concatenated_code"], source_var],
                                                                      data_frames[data_set].loc[arguments["fips_concatenated_code"], calc_var]],
                                                                     arguments, self.data_files, 2)
                            else:
                                element_data_frame.loc[index, data_element.variable_name] = \
                                    value_getter.get_acs_calculation(data_element.variable_name,
                                                                     data_frames[data_set].loc[arguments["fips_concatenated_code"],
                                                                         data_element.source_variable], arguments, self.data_files, 2)
                        else:
                            element_data_frame.loc[index, data_element.variable_name] = \
                                data_frames[data_set].loc[arguments["fips_concatenated_code"], data_element.source_variable]
                element_data_frame.drop(columns=['Unnamed: 0'], inplace=True)
                if os.path.exists(excel_path):
                    book = load_workbook(excel_path)
                    writer = pd.ExcelWriter(excel_path, engine='openpyxl')
                    writer.book = book
                    element_data_frame.to_excel(writer, sheet_name=data_element.sheet_name)
                    writer.save()
                    writer.close()
                else:
                    writer = pd.ExcelWriter(excel_path, engine='openpyxl')
                    element_data_frame.to_excel(writer, sheet_name=data_element.sheet_name)
                    writer.save()
                    writer.close()
        for data_element in self.non_acs_data_elements:
            element_data_frame = self.data_frame.copy()
            element_data_frame[data_element.variable_name] = ''
            element_data_frame.drop(element_data_frame.index[element_data_frame['address_start_date'] > self.data_files[data_element.data_source][-1].end_date], inplace=True)
            element_data_frame.drop(element_data_frame.index[element_data_frame['address_end_date'] <= self.data_files[data_element.data_source][0].start_date], inplace=True)
            element_data_frame.reset_index(drop=True, inplace=True)
            for i, data_source in enumerate(self.data_files[data_element.data_source]):
                for index, row in element_data_frame.iterrows():
                    arguments = {"fips_concatenated_code": element_data_frame.loc[index, constant.GEO_ID_NAME],
                                 "latitude": element_data_frame.loc[index, "latitude"],
                                 "longitude": element_data_frame.loc[index, "longitude"]}
                    if not arguments["fips_concatenated_code"] == constant.ADDRESS_NOT_GEOCODABLE:
                        if i == 0 and element_data_frame.loc[index, 'address_start_date'] < data_source.start_date:
                            element_data_frame.loc[index, 'address_start_date'] = data_source.start_date
                        if data_source.start_date <= element_data_frame.loc[index, 'address_start_date'] <= data_source.end_date:
                            element_data_frame.loc[index, data_element.variable_name] = value_getter.get_value(data_element, arguments, data_source, 2)
                            if element_data_frame.loc[index, 'address_end_date'] > data_source.end_date:
                                if i + 1 == len(self.data_files[data_element.data_source]):
                                    element_data_frame.loc[index, 'address_end_date'] = data_source.end_date
                                else:
                                    new_row = element_data_frame.iloc[[index]].copy()
                                    new_row.loc[index, 'address_start_date'] = data_source.end_date + timedelta(days=1)
                                    element_data_frame.loc[index, 'address_end_date'] = data_source.end_date
                                    element_data_frame = pd.concat([element_data_frame, new_row], ignore_index=True)
            element_data_frame.drop(columns=['Unnamed: 0'], inplace=True)
            if os.path.exists(excel_path):
                book = load_workbook(excel_path)
                writer = pd.ExcelWriter(excel_path, engine='openpyxl')
                writer.book = book
                element_data_frame.to_excel(writer, sheet_name=data_element.sheet_name)
                writer.save()
                writer.close()
            else:
                writer = pd.ExcelWriter(excel_path, engine='openpyxl')
                element_data_frame.to_excel(writer, sheet_name=data_element.sheet_name)
                writer.save()
                writer.close()

    def enhance(self):
        self.load_enhancement_job()
        if self.version == 1:
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