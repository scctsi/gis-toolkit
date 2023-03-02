import pytest
import geocoder
import importer
import main
import sedoh_data_structure as sds
from data_frame_enhancer import DataFrameEnhancer
import os, shutil, errno, stat
import constant
from config import input_config


def handle_remove_readonly(func, path, exc):
    excvalue = exc[1]
    if func in (os.rmdir, os.remove) and excvalue.errno == errno.EACCES:
        os.chmod(path, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)  # 0777
        func(path)
    else:
        raise


@pytest.fixture(autouse=True)
def run_around_tests():
    yield
    if os.path.exists('./temp'):
        shutil.rmtree('./temp', ignore_errors=False, onerror=handle_remove_readonly)


def test_enhancement_validity():
    data_elements = sds.SedohDataElements().data_elements
    data_files = sds.DataFiles().data_files
    file_path = './validation/addresses-us-all.csv'
    data_key = main.get_data_key(file_path)
    input_data_frame = importer.import_file(file_path)
    input_data_frame = input_data_frame[94:95]
    input_data_frame.reset_index(drop=True, inplace=True)
    input_data_frame = geocoder.geocode_addresses_in_data_frame(input_data_frame, data_key, version='latest')
    sedoh_enhancer = DataFrameEnhancer(input_data_frame, data_elements, data_files, data_key, version='latest', test_mode=True)
    enhanced_data_frame = sedoh_enhancer.enhance()
    control_data_frame = importer.import_file('./tests/enhancement_control.csv')
    for data_element in data_elements:
        print(data_element.variable_name)
        assert enhanced_data_frame.iloc[0][data_element.variable_name] == \
                control_data_frame.iloc[0][data_element.variable_name]


def test_input_file_validation():
    input_data_frame_v1 = importer.import_file('./tests/input_file_validation_v1.csv', version='latest')
    input_data_frame_v2 = importer.import_file('./tests/input_file_validation_v2.csv', version='comprehensive')
    try:
        main.input_file_validation(input_data_frame_v1, version='latest', geocode="geocode")
    except Exception:
        pytest.fail("input_file_validation() failed with version latest")
    try:
        main.input_file_validation(input_data_frame_v2, version='comprehensive', geocode=None)
    except Exception:
        pytest.fail("input_file_validation() failed with version comprehensive")


def test_geocodable_address():
    file_path = './validation/addresses-us-all.csv'
    data_key = main.get_data_key(file_path)
    input_data_frame = importer.import_file(file_path)
    input_data_frame = input_data_frame.iloc[50:52]
    input_data_frame.index = [0, 1]
    input_data_frame = geocoder.geocode_addresses_in_data_frame(input_data_frame, data_key, version='latest')
    assert input_data_frame.iloc[0][input_config["geo_id_name"]] == "04013618000"


def test_non_geocodable_address():
    file_path = './validation/addresses-us-all.csv'
    data_key = main.get_data_key(file_path)
    input_data_frame = importer.import_file(file_path)
    input_data_frame = input_data_frame.iloc[50:52]
    input_data_frame.index = [0, 1]
    input_data_frame = geocoder.geocode_addresses_in_data_frame(input_data_frame, data_key, version='latest')
    assert input_data_frame.iloc[1][input_config["geo_id_name"]] == constant.ADDRESS_NOT_GEOCODABLE


def test_comprehensive_geocoding():
    file_path = './tests/comprehensive_geocoding_input.csv'
    data_key = main.get_data_key(file_path)
    input_data_frame = importer.import_file(file_path, version='comprehensive')
    geocoded_data_frame = geocoder.geocode_addresses_in_data_frame(input_data_frame, data_key, version='comprehensive')
    comprehensive_output = [3, 2, 2, 1, 0]
    for index, row in input_data_frame.iterrows():
        address_count = geocoded_data_frame.index[geocoded_data_frame[input_config["street"]] == row[input_config["street"]]].tolist()
        assert len(address_count) == comprehensive_output[index]


def test_comprehensive_coordinate_geocoding():
    file_path = './tests/comprehensive_geocoding_input.csv'
    data_key = main.get_data_key(file_path)
    data_frame = importer.import_file(file_path, version='comprehensive')
    geocoded_data_frame = geocoder.geocode_data_frame(data_frame.copy(), data_key, 'latest')
    address_data_frame = geocoder.geocode_data_frame(data_frame.copy(), data_key, 'comprehensive')
    coordinate_data_frame = geocoded_data_frame.drop([input_config["street"], input_config["city"], input_config["state"], input_config["zip"], input_config["geo_id_name"]], axis=1)
    coordinate_data_frame = geocoder.geocode_data_frame(coordinate_data_frame, data_key, 'comprehensive')
    assert address_data_frame[input_config["geo_id_name"]].tolist() == coordinate_data_frame[input_config["geo_id_name"]].tolist()
