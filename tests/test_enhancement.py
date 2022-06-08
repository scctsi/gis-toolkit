import pytest
import geocoder
import importer
import main
import sedoh_data_structure as sds
from data_frame_enhancer import DataFrameEnhancer
import os, shutil, errno, stat
from data_structure import GetStrategy


def handle_remove_readonly(func, path, exc):
    excvalue = exc[1]
    if func in (os.rmdir, os.remove) and excvalue.errno == errno.EACCES:
        os.chmod(path, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)  # 0777
        func(path)
    else:
        raise


def load_data_files():
    data_files = {
        sds.SedohDataSource.CalEPA_CES: (importer.import_file("./data_files/calepa_ces/calepa_ces_3.0.xlsx"), "Census Tract"),
        sds.SedohDataSource.CDC: (importer.import_file("./data_files/cdc/cdc_2018.csv"), "FIPS"),
        sds.SedohDataSource.Gazetteer: (importer.import_file("./tests/gazetteer.csv"), "GEOID"),
        sds.SedohDataSource.USDA: (importer.import_file('./data_files/usda/usda.xls'), "CensusTrac")
    }

    # TODO: This is a fix to add a leading 0 to the CalEPA_CES data file. Get the data from CalEPA to fix this issue.
    calepa_ces_data_file = data_files[sds.SedohDataSource.CalEPA_CES][0]
    calepa_ces_data_file['Census Tract'] = '0' + calepa_ces_data_file['Census Tract']

    return data_files


def test_enhancement_validity():
    data_elements = sds.SedohDataElements().data_elements
    data_files = load_data_files()
    file_path = './validation/addresses-us-all.csv'
    data_key = main.get_data_key(file_path)
    input_data_frame = importer.import_file(file_path)
    input_data_frame = input_data_frame[:3]
    input_data_frame = geocoder.geocode_addresses_in_data_frame(input_data_frame, data_key, version=1)
    sedoh_enhancer = DataFrameEnhancer(input_data_frame, data_elements, data_files, data_key, version=1, test_mode=True)
    enhanced_data_frame = sedoh_enhancer.enhance().iloc[[2]]
    control_data_frame = importer.import_file('./tests/enhancement_control.csv')
    for data_element in data_elements:
        print(data_element.variable_name)
        # print("newly enhanced:  ", enhanced_data_frame.iloc[0][data_element.variable_name])
        # print("control:         ", control_data_frame.iloc[0][data_element.variable_name])
        if data_element.get_strategy != GetStrategy.RASTER_FILE:
            assert enhanced_data_frame.iloc[0][data_element.variable_name] == \
                control_data_frame.iloc[0][data_element.variable_name]


def test_input_file_validation():
    input_data_frame_v1 = importer.import_file('./tests/input_file_validation_v1.csv', version=1)
    input_data_frame_v2 = importer.import_file('./tests/input_file_validation_v2.csv', version=2)
    try:
        main.input_file_validation(input_data_frame_v1, 1)
    except Exception:
        pytest.fail("input_file_validation() failed with version 1")
    try:
        main.input_file_validation(input_data_frame_v2, 2)
    except Exception:
        pytest.fail("input_file_validation() failed with version 2")