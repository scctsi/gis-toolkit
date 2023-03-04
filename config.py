import json


def add_input_column_names():
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


def add_enhancement_variables():
    import sedoh_data_structure as sds
    data_elements = sds.SedohDataElements().data_elements
    enhancement_variables = {data_element.variable_name: {"enhance": True, "output_column_name": data_element.variable_name, "output_sheet_name": data_element.sheet_name} for data_element in data_elements}
    return enhancement_variables


def add_config_key():
    config = {}
    config.update({"input_column_names": add_input_column_names()})
    config.update({"enhancement_variables": add_enhancement_variables()})
    return config


def write_config_key():
    config_key = add_config_key()
    with open("./config.json", "w+") as key_file:
        json.dump(config_key, key_file, indent=4)


def read_config_key():
    with open("./config.json", "r+") as key_file:
        config = json.load(key_file)
    return config


def get_input_column_names(config):
    return config["input_column_names"]


def get_enhancement(config):
    enhancement = {variable_name: options["enhance"] for variable_name, options in config["enhancement_variables"].items()}
    return enhancement


def get_output_column_names(config):
    output_column_names = {variable_name: options["output_column_name"] for variable_name, options in config["enhancement_variables"].items()}
    return output_column_names


def get_output_sheet_names(config):
    output_sheet_names = {variable_name: options["output_sheet_name"] for variable_name, options in config["enhancement_variables"].items()}
    return output_sheet_names


config_key = read_config_key()
input_config = get_input_column_names(config_key)
enhancement_config = get_enhancement(config_key)
output_columns_config = get_output_column_names(config_key)
output_sheets_config = get_output_sheet_names(config_key)
