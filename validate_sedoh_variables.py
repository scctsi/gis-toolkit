import random
import constant
from data_frame_enhancer import GeoenhancedCache
from data_structure import GetStrategy
import sedoh_data_structure as sds
import value_getter
import pandas as pd
import main
import json
import csv
import math


def validate_sedoh_data_element(data_element, data_files, api_validation_range=5):
    with open('./private_test_files/validation_source_key.json', 'r+') as key_file:
        data = json.load(key_file)
    validation_file = pd.ExcelFile('./private_test_files/GIS_Measurement_V2_03-04-2022_Validation.xlsx')
    true_data_frame = pd.read_excel(validation_file, data[data_element.friendly_name], dtype='str')
    total = 0
    matched = 0
    empty_cache = GeoenhancedCache()
    if data_element.get_strategy == GetStrategy.FILE:
        for index, row in true_data_frame.iterrows():
            arguments = {"fips_concatenated_code": true_data_frame.iloc[index][constant.GEO_ID_NAME]}
            source_value = true_data_frame.iloc[index]["NUMERIC"]
            data_value = value_getter.get_value(data_element, arguments, data_files, empty_cache)
            if data_value is not constant.NOT_AVAILABLE and (not math.isnan(float(source_value))):
                total += 1
                if abs(float(source_value) - float(data_value)) < .01:
                    matched += 1
                # else:
                #     print(source_value, data_value)
    elif data_element.get_strategy in [GetStrategy.PRIVATE_API, GetStrategy.CALCULATION]:
        section = int(len(true_data_frame) / api_validation_range)
        while total < api_validation_range:
            index = random.randint(total * section, (total + 1) * section)
            arguments = {"fips_concatenated_code": true_data_frame.iloc[index][constant.GEO_ID_NAME]}
            source_value = true_data_frame.iloc[index]["NUMERIC"]
            print(source_value)
            data_value = value_getter.get_value(data_element, arguments, data_files, empty_cache)
            if data_value is not constant.NOT_AVAILABLE and (not math.isnan(float(source_value))):
                total += 1
                if abs(float(source_value) - float(data_value)) < .01:
                    matched += 1
                # else:
                #     print(source_value, data_value)
    if total > 0:
        result = round((matched / total) * 100, 3)
    else:
        result = -1
    return result

data_elements = sds.SedohDataElements().data_elements
data_files = main.load_data_files()
validation_results = []
for data_element in data_elements:
    if data_element.friendly_name in ['Percent Below 200% of Fed Poverty Level', 'Percent Below 300% of Fed Poverty Level']:
        validation_results.append([data_element.friendly_name, validate_sedoh_data_element(data_element, data_files)])

print(validation_results)

# header = ['variable', 'percent_accurate']
# with open('./private_files/validation_results.csv', 'w', newline='') as result_file:
#     writer = csv.writer(result_file)
#     writer.writerow(header)
#     writer.writerows(validation_results)





