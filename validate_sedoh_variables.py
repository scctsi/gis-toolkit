import constant
from data_structure import GetStrategy
import sedoh_data_structure as sds
import value_getter
import pandas as pd
import main
import csv

data_elements = sds.SedohDataElements().data_elements
file_read_data_elements = []
data_files = main.load_data_files()
for data_element in data_elements:
    if data_element.get_strategy == GetStrategy.FILE:
        file_read_data_elements.append(data_element)

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
        data_value = value_getter.get_value(file_read_data_elements[i], arguments, data_files)
        if data_value is not constant.NOT_AVAILABLE:
            total += 1
            if i != 5:
                if true_data_frame.iloc[index]["NUMERIC"] == str(round(float(data_value), 4)):
                    matched += 1
            else:
                if true_data_frame.iloc[index]["NUMERIC"] == data_value:
                    matched += 1
    validation_results.append([file_read_data_elements[i].variable_name, round((matched / total) * 100, 5)])

header = ['variable', 'percent_accurate']
with open('./validation/validation_results.csv', 'w', newline='') as result_file:
    writer = csv.writer(result_file)
    writer.writerow(header)
    writer.writerows(validation_results)





