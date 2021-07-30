import os

import requests
from dotenv import load_dotenv

from data_structure import GetStrategy
from sedoh_data_structure import SedohDataSource

load_dotenv()


# Note on naming scheme: parameters = named variables passed to a function,
#                        arguments = expressions used when calling the function,
#                        options = optional arguments which allow yoy to customize the function


def get_value(data_element, arguments, data_files):
    if data_element.data_source == SedohDataSource.ACS:
        if data_element.get_strategy == GetStrategy.PRIVATE_API:
            return get_acs_value(data_element.source_variable, arguments)
        elif data_element.get_strategy == GetStrategy.CALCULATION:
            # TODO: Refactor this code to use a list for source variables
            source_value = get_acs_value(data_element.source_variable, arguments)
            return get_acs_calculation(data_element.variable_name, source_value)
        else:
            return None
            # TODO: raise error
    elif data_element.get_strategy == GetStrategy.FILE:
        # if type(data_element.source_variable) == list:
        #     else:
        #
        return get_file_value(data_element.source_variable,
                              arguments,
                              data_files[data_element.data_source][0],
                              data_files[data_element.data_source][1])
    elif data_element.get_strategy == GetStrategy.FILE_AND_CALCULATION:
        return get_calculated_file_value(data_element.source_variable,
                                         arguments,
                                         data_files[data_element.data_source][0],
                                         data_files[data_element.data_source][1],
                                         data_element.variable_name)


# ACS specific methods
def construct_geography_argument(arguments):
    if "tract_code" in arguments and "state_code" in arguments and "county_code" in arguments:
        state_code = arguments['state_code']
        county_code = arguments['county_code']
        tract_code = arguments['tract_code']
    elif "fips_concatenated_code" in arguments:  # TODO: Come up with better name for fips_concatenated_code
        state_code = arguments["fips_concatenated_code"][0:2]
        county_code = arguments["fips_concatenated_code"][2:5]
        tract_code = arguments["fips_concatenated_code"][5:11]
    else:
        # TODO: Raise error here, but this shouldn't happen.
        state_code = ""
        county_code = ""
        tract_code = ""

    return f"for=tract:{tract_code}&in=state:{state_code}&in=county:{county_code}"


def get_acs_dataset_name(source_variable):
    if source_variable[0].lower() == 's':
        return 'acs/acs5/subject'
    elif source_variable[0:2].lower() == 'dp':
        return 'acs/acs5/profile'
    else:
        return "acs/acs5"


def get_acs_value(source_variable, arguments):
    # The vintage year (e.g., V2019) refers to the final year of the time series.
    # The reference date for all estimates is July 1, unless otherwise specified.
    api_parameters = {
        "host_name": "https://api.census.gov/data",
        "data_year": "2018",
        "dataset_name": get_acs_dataset_name(source_variable),
        "variables": source_variable,
        "geographies": construct_geography_argument(arguments),
        "key": os.getenv("census_api_key")
    }

    census_api_interpolation_string = "{host_name}/{data_year}/{dataset_name}?get={variables}&{geographies}" \
                                      "&key={key}"
    api_url = construct_api_url(census_api_interpolation_string, api_parameters)
    return get_api_value(api_url)


def get_acs_values(source_variables, arguments):
    # The vintage year (e.g., V2019) refers to the final year of the time series.
    # The reference date for all estimates is July 1, unless otherwise specified.
    api_parameters = {
        "host_name": "https://api.census.gov/data",
        "data_year": "2018",
        "dataset_name": 'acs/acs5/subject',
        "variables": source_variables,
        "geographies": construct_geography_argument(arguments),
        "key": os.getenv("census_api_key")
    }

    census_api_interpolation_string = "{host_name}/{data_year}/{dataset_name}?get={variables}&{geographies}" \
                                      "&key={key}"
    api_url = construct_api_url(census_api_interpolation_string, api_parameters)
    get_api_values(api_url)


def get_acs_calculation(variable_name, source_value):
    # TODO: Change from using hardcoded variable_name checks
    if variable_name == 'housing_percent_occupied_units_lacking_plumbing':
        return 100 - source_value
    elif variable_name == 'housing_percent_occupied_lacking_complete_kitchen':
        return 100 - source_value
    else:
        return None


# API specific methods
def construct_api_url(interpolation_string, arguments):
    return interpolation_string.format(**arguments)


def get_api_response(url):
    # TODO: Assert 200
    response = requests.get(url)
    return response.json()


def get_header_row_and_truncated_json(json_to_process):
    header_row = json_to_process[0].copy()
    del json_to_process[0]

    return header_row, json_to_process


def get_api_value(url):
    response = get_api_response(url)
    header_row, truncated_response = get_header_row_and_truncated_json(response)
    # TODO: This is specific to ACS which returns JSON like this example below:
    # [['B19083_001E', 'state', 'county', 'tract'], ['0.4981', '06', '001', '400100']]
    # For now, only ACS has an API, but this function needs expansion once we have more API data sources
    return truncated_response[0][0]


def get_api_values(url):
    # This call is currently just used for benchmarking
    get_api_response(url)


# File specific methods
def get_file_value(source_variable, arguments, data_file, data_file_search_column_name):
    # TODO: Fix the naming and the intent of this function
    indexes = data_file.index[data_file[data_file_search_column_name] == arguments["fips_concatenated_code"]].tolist()

    if len(indexes) == 0:
        return "Missing"
    elif len(indexes) == 1:
        return data_file.iloc[indexes[0]][source_variable]
    else:
        return "Error"


def get_calculated_file_value(source_variables, arguments, data_file, data_file_search_column_name, variable_name):
    source_values = {}

    for source_variable in source_variables:
        indexes = data_file.index[
            data_file[data_file_search_column_name] == arguments["fips_concatenated_code"]].tolist()
        source_values[source_variable] = data_file.iloc[indexes[0]][source_variable]

    # TODO: Refactor these condition based calculations
    if variable_name == 'food_fraction_of_population_with_low_access':
        if source_values['Urban'] == '1':
            return source_values['lapop1shar']
        else:
            return source_values['lapop10sha']

    print(source_values)
    return None
