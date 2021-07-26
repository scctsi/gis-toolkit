import os

import requests
from dotenv import load_dotenv

from data_structure import GetStrategy
from sedoh_data_structure import SedohDataSource

load_dotenv()

# Note on naming scheme: parameters = named variables passed to a function,
#                        arguments = expressions used when calling the function,
#                        options = optional arguments which allow yoy to customize the function


def get_value(data_element, arguments):
    if data_element.source == SedohDataSource.ACS:
        if data_element.get_strategy == GetStrategy.PUBLIC_API:
            return get_acs_value(data_element.source_variable, arguments)


# ACS specific methods
def construct_geography_argument(arguments):
    if "tract_code" in arguments and "state_code" in arguments and "county_code" in arguments:
        state_code = arguments['state_code']
        county_code = arguments['county_code']
        tract_code = arguments['tract_code']
    elif "fips_concatenated_code" in arguments: # TODO: Ask Beau for better name
        state_code = arguments["fips_concatenated_code"][0:2]
        county_code = arguments["fips_concatenated_code"][2:5]
        tract_code = arguments["fips_concatenated_code"][5:11]
    else:
        # TODO: Raise error here, but this shouldn't happen.
        state_code = ""
        county_code = ""
        tract_code = ""

    return f"for=tract:{tract_code}&in=state:{state_code}&in=county:{county_code}"


def get_acs_dataset_name(variable_name):
    if variable_name[0].lower() == 's':
        return 'acs/acs5/subject'
    elif variable_name[0:2].lower() == 'dp':
        return 'acs/acs5/profile'
    else:
        return "acs/acs5"


def get_acs_value(variable_name, arguments):
    # census_api_interpolation_string = \
    #     "https://api.census.gov/data/2018/acs/acs5?get=NAME,B19083_001E,B19083_001M&for=tract:*&in=state:06"

    # The vintage year (e.g., V2019) refers to the final year of the time series.
    # The reference date for all estimates is July 1, unless otherwise specified.
    api_parameters = {
        "host_name": "https://api.census.gov/data",
        "data_year": "2018",
        "dataset_name": get_acs_dataset_name(variable_name),
        "variables": variable_name,
        "geographies": construct_geography_argument(arguments),
        "key": os.getenv("census_api_key")
    }

    census_api_interpolation_string = "{host_name}/{data_year}/{dataset_name}?get={variables}&{geographies}" \
                                      "&key={key}"
    api_url = construct_api_url(census_api_interpolation_string, api_parameters)
    return get_api_value(api_url)


# API specific methods
def construct_api_url(interpolation_string, parameters):
    return interpolation_string.format(**parameters)


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


# def get_api_values(response, has_header_row=False):
#     # Sample FIPS code: '06001400100'
#     api_values = []
#
#     response_json = response.json()
#
#     if has_header_row:
#         processed_json = get_header_row_and_truncated_json(response_json)
#         header_row = processed_json[0]
#         response_json = processed_json[1]
#
#         for row in response_json:
#             for header_column, index in enumerate(header_row):
#                 a = 1
#
#     # print(response_json)
#     return 0
