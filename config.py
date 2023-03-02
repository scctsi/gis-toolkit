import json
import sedoh_data_structure as sds


def get_input_column_names():
    input_column_names = {}
    input_column_names.update({"geo_id_name": "SPATIAL_GEOID"})
    input_column_names.update({"address_start_date": "address_start_date"})
    input_column_names.update({"address_end_date": "address_end_date"})
    input_column_names.update({"latitude": "latitude"})
    input_column_names.update({"longitude": "longitude"})
    input_column_names.update({"street": "street"})
    input_column_names.update({"city": "city"})
    input_column_names.update({"state": "state"})
    input_column_names.update({"zip": "zip"})
    return input_column_names


def get_enhancement_variables():
    data_elements = sds.SedohDataElements().data_elements
    enhancement_variables = {data_element.variable_name: True for data_element in data_elements}
    return enhancement_variables


def get_config_key():
    config = {}
    config.update({"input_column_names": get_input_column_names()})
    config.update({"enhancement_variables": get_enhancement_variables()})
    return config


def write_config_key():
    config_key = get_config_key()
    with open("./config.json", "w+") as key_file:
        json.dump(config_key, key_file, indent=4)


def read_config_key():
    with open("./config.json", "r+") as key_file:
        config_key = json.load(key_file)
    return config_key


def flatten_dict(flat_dict, partial_dict):
    for key, value in partial_dict.items():
        flat_dict.update({key: value})
    return flat_dict


def get_flat_config_key():
    config_key = read_config_key()
    flat_config_key = {}
    flatten_dict(flat_config_key, config_key["input_column_names"])
    return flat_config_key


config = read_config_key()
input_config = config["input_column_names"]
enhancement_config = config["enhancement_variables"]
