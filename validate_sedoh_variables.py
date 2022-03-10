import random

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
    if data_element.data_source == sds.SedohDataSource.ACS and data_element.get_strategy == GetStrategy.PRIVATE_API:
    # if data_element.get_strategy == GetStrategy.FILE:
        filtered_data_elements.append(data_element)
# print(len(filtered_data_elements))

validation_file = pd.ExcelFile('./validation/GIS_Measurement_V2_03-04-2022_Validation.xlsx')
true_data_frames = []
# true_data_frames.append(pd.read_excel(validation_file, 'SVI', dtype='str'))
# true_data_frames.append(pd.read_excel(validation_file, 'OZONE', dtype='str'))
# true_data_frames.append(pd.read_excel(validation_file, 'PM25', dtype='str'))
# true_data_frames.append(pd.read_excel(validation_file, 'WATER', dtype='str'))
# true_data_frames.append(pd.read_excel(validation_file, 'ASTHMA', dtype='str'))
# true_data_frames.append(pd.read_excel(validation_file, 'FOODACC_TRACT', dtype='str'))
true_data_frames.append(pd.read_excel(validation_file, 'GINI', dtype='str'))
true_data_frames.append(pd.read_excel(validation_file, 'POP_OLD', dtype='str'))
true_data_frames.append(pd.read_excel(validation_file, 'POP_CHILD', dtype='str'))
true_data_frames.append(pd.read_excel(validation_file, 'HU_AGE', dtype='str'))
true_data_frames.append(pd.read_excel(validation_file, 'HU_BEDRMS', dtype='str'))
true_data_frames.append(pd.read_excel(validation_file, 'HU_VEHICLE', dtype='str'))
true_data_frames.append(pd.read_excel(validation_file, 'HU_COMP', dtype='str'))
true_data_frames.append(pd.read_excel(validation_file, 'HU_INT', dtype='str'))
true_data_frames.append(pd.read_excel(validation_file, 'HISP_Y', dtype='str'))
true_data_frames.append(pd.read_excel(validation_file, 'HISP_N', dtype='str'))
true_data_frames.append(pd.read_excel(validation_file, 'RACE_1', dtype='str'))
true_data_frames.append(pd.read_excel(validation_file, 'RACE_2', dtype='str'))
true_data_frames.append(pd.read_excel(validation_file, 'RACE_3', dtype='str'))
true_data_frames.append(pd.read_excel(validation_file, 'RACE_4', dtype='str'))
true_data_frames.append(pd.read_excel(validation_file, 'RACE_6', dtype='str'))
true_data_frames.append(pd.read_excel(validation_file, 'RACE_5', dtype='str'))
true_data_frames.append(pd.read_excel(validation_file, 'RACE_OT', dtype='str'))
true_data_frames.append(pd.read_excel(validation_file, 'INC_100FPL', dtype='str'))
true_data_frames.append(pd.read_excel(validation_file, 'FOOD_SNAP', dtype='str'))
true_data_frames.append(pd.read_excel(validation_file, 'LTD_ENG', dtype='str'))
true_data_frames.append(pd.read_excel(validation_file, 'EDU_HS', dtype='str'))
true_data_frames.append(pd.read_excel(validation_file, 'EDU_BDEG', dtype='str'))
true_data_frames.append(pd.read_excel(validation_file, 'INC_MHH', dtype='str'))
true_data_frames.append(pd.read_excel(validation_file, 'UNEMP', dtype='str'))
validation_results = []
validation_range = 5
for i, true_data_frame in enumerate(true_data_frames):
    total = 0
    matched = 0
    section = int(len(true_data_frame)/validation_range)
    while total < validation_range:
        index = random.randint(total * section, (total + 1) * section)
        arguments = {"fips_concatenated_code": true_data_frame.iloc[index][constant.GEO_ID_NAME]}
        data_value = value_getter.get_value(filtered_data_elements[i], arguments, data_files)
        if data_value is not constant.NOT_AVAILABLE and (not math.isnan(float(true_data_frame.iloc[index]['NUMERIC']))) and (not math.isnan(float(data_value))):
            if i != 3:
                total += 1
                if abs(float(true_data_frame.iloc[index]["NUMERIC"]) - float(data_value)) < .01:
                    matched += 1
                else:
                    print(true_data_frame.iloc[index]['NUMERIC'], data_value)
            else:
                if data_value != "0":
                    total += 1
                    if abs(float(true_data_frame.iloc[index]["NUMERIC"]) - float(data_value)) < .01:
                        matched += 1
                    else:
                        print(true_data_frame.iloc[index]['NUMERIC'], data_value)
    # if i == 7 or i == 21:
    #     print(filtered_data_elements[i].friendly_name)
    # for index, row in true_data_frame.iterrows():
    #     arguments = {"fips_concatenated_code": true_data_frame.iloc[index][constant.GEO_ID_NAME]}
    #     data_value = value_getter.get_value(filtered_data_elements[i], arguments, data_files)
    #     if data_value is not constant.NOT_AVAILABLE and (not math.isnan(float(true_data_frame.iloc[index]['NUMERIC']))) and (not math.isnan(float(data_value))):
    #         if i != 3:
    #             total += 1
    #             if abs(float(true_data_frame.iloc[index]["NUMERIC"]) - float(data_value))<.01:
    #                 matched += 1
    #             else:
    #                 print(true_data_frame.iloc[index]['NUMERIC'], data_value)
    #         else:
    #             if data_value != "0":
    #                 total += 1
    #                 if abs(float(true_data_frame.iloc[index]["NUMERIC"]) - float(data_value)) < .01:
    #                     matched += 1
    #                 else:
    #                     print(true_data_frame.iloc[index]['NUMERIC'], data_value)
    validation_results.append([filtered_data_elements[i].variable_name, round((matched / total) * 100, 5)])

header = ['variable', 'percent_accurate']
with open('./validation/acs_validation_results.csv', 'w', newline='') as result_file:
    writer = csv.writer(result_file)
    writer.writerow(header)
    writer.writerows(validation_results)





