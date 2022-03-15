import constant
from data_structure import GetStrategy
import sedoh_data_structure as sds
import value_getter
import pandas as pd
import main
import csv
import math

data_elements = sds.SedohDataElements().data_elements
filtered_data_elements = []
data_files = main.load_data_files()
for data_element in data_elements:
    if data_element.get_strategy == GetStrategy.FILE:
        filtered_data_elements.append(data_element)

validation_file = pd.ExcelFile('./validation/GIS_Measurement_V2_03-04-2022_Validation.xlsx')
true_data_frames = []
true_data_frames.append(pd.read_excel(validation_file, 'SVI', dtype='str'))
true_data_frames.append(pd.read_excel(validation_file, 'OZONE', dtype='str'))
true_data_frames.append(pd.read_excel(validation_file, 'PM25', dtype='str'))
true_data_frames.append(pd.read_excel(validation_file, 'WATER', dtype='str'))
true_data_frames.append(pd.read_excel(validation_file, 'ASTHMA', dtype='str'))
true_data_frames.append(pd.read_excel(validation_file, 'FOODACC_TRACT', dtype='str'))

validation_results = []
for i, true_data_frame in enumerate(true_data_frames):
    total = 0
    matched = 0
    for index, row in true_data_frame.iterrows():
        arguments = {"fips_concatenated_code": true_data_frame.iloc[index][constant.GEO_ID_NAME]}
        data_value = value_getter.get_value(filtered_data_elements[i], arguments, data_files)
        if data_value is not constant.NOT_AVAILABLE and (not math.isnan(float(true_data_frame.iloc[index]['NUMERIC']))) and (not math.isnan(float(data_value))):
            total += 1
            if abs(float(true_data_frame.iloc[index]["NUMERIC"]) - float(data_value))<.01:
                matched += 1
            else:
                print(true_data_frame.iloc[index]['NUMERIC'], data_value)
    validation_results.append([filtered_data_elements[i].variable_name, round((matched / total) * 100, 5)])

header = ['variable', 'percent_accurate']
with open('./validation/file_based_validation_results.csv', 'w', newline='') as result_file:
    writer = csv.writer(result_file)
    writer.writerow(header)
    writer.writerows(validation_results)





