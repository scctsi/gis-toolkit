import os
import pandas as pd
import api
from dotenv import load_dotenv
import math
import sedoh_data_structure as sds
from data_structure import GetStrategy
from sedoh_data_structure import SedohDataSource
import constant

load_dotenv()


# Note on naming scheme: parameters = named variables passed to a function,
#                        arguments = expressions used when calling the function,
#                        options = optional arguments which allow yoy to customize the function


def join_gazetteer_source(data_frame, data_files, version):
    gazetteer_index = -1
    if version == 2:
        for i, data_source in enumerate(data_files[sds.SedohDataSource.Gazetteer]):
            if data_source.start_date <= data_frame.loc[0, constant.ADDRESS_START_DATE] < data_source.end_date:
                gazetteer_index = i
                break
    enhancer_data_frame = get_enhancer_data_frame(data_files[sds.SedohDataSource.Gazetteer][gazetteer_index])
    data_frame = data_frame.join(enhancer_data_frame, on=constant.GEO_ID_NAME, rsuffix='_other')
    return data_frame


def get_enhancer_data_frame(data_source):
    enhancer_data_frame = data_source.data_frame.copy()
    enhancer_data_frame.rename(columns={data_source.tract_column: constant.GEO_ID_NAME}, inplace=True)
    enhancer_data_frame.index = enhancer_data_frame[constant.GEO_ID_NAME]
    return enhancer_data_frame


def complementary_percent(percent):
    return str(100 - float(percent))


def percent_below_fed_poverty_level(numerator, denominator):
    return str(round(100 * float(numerator) / float(denominator), 2))


def population_density(population, area_land):
    if math.isnan(float(area_land)) or int(area_land) == 0:
        return constant.NOT_AVAILABLE
    else:
        return str(round(1000000 * float(population) / int(area_land), 0))


def food_fraction_with_low_access(urban, la_pop_one, la_pop_ten):
    if urban == '1':
        return str(float(la_pop_one) * 100)
    else:
        return str(la_pop_ten)


def calculate_raster_value(latitude, longitude, raster_source):
    latitude = round(float(latitude), raster_source.precision)
    longitude = round(float(longitude), raster_source.precision)
    if raster_source.latitude_range[0] <= latitude <= raster_source.latitude_range[1] and raster_source.longitude_range[
        0] <= longitude <= raster_source.longitude_range[1]:
        latitude_difference = round(latitude - raster_source.latitude_range[0], raster_source.precision)
        longitude_difference = round(longitude - raster_source.longitude_range[0], raster_source.precision)
        latitude_index = raster_source.latitude_transform - int(round(latitude_difference / raster_source.step, 2))
        longitude_index = int(round(longitude_difference / raster_source.step, 2))
        value = raster_source.array[latitude_index][longitude_index]
        if value != -99:
            return value
    return constant.NOT_AVAILABLE


def get_lambda_calculation(data_frame, variable_name):
    if variable_name in ['housing_percent_occupied_units_lacking_plumbing', 'housing_percent_occupied_lacking_complete_kitchen']:
        data_frame[variable_name] = data_frame.apply(lambda col: complementary_percent(col[variable_name]), axis=1)
    elif variable_name == "percent_below_200_of_fed_poverty_level":
        data_frame[variable_name] = data_frame.apply(lambda col: percent_below_fed_poverty_level(col['S1701_C01_042E'], col['S1701_C01_001E']), axis=1)
    elif variable_name == "percent_below_300_of_fed_poverty_level":
        data_frame[variable_name] = data_frame.apply(lambda col: percent_below_fed_poverty_level(col['S1701_C01_043E'], col['S1701_C01_001E']), axis=1)
    elif variable_name == "population_density":
        data_frame[variable_name] = data_frame.apply(lambda col: population_density(col[variable_name], col['ALAND']), axis=1)
    elif variable_name == "food_fraction_of_population_with_low_access":
        data_frame[variable_name] = data_frame.apply(lambda col: food_fraction_with_low_access(col['Urban'], col['lapop1share'], col['lapop10share']), axis=1)
    return data_frame


def enhance_data_element(data_frame, enhancer_data_frame, data_element, data_files, version=1):
    idx_non_geocoded = data_frame.index[data_frame[constant.GEO_ID_NAME] == constant.ADDRESS_NOT_GEOCODABLE]
    non_geocoded_data_frame = data_frame.loc[idx_non_geocoded]
    non_geocoded_data_frame[data_element.variable_name] = ''
    data_frame.drop(idx_non_geocoded, inplace=True)

    idx_missing = data_frame.index[data_frame[constant.GEO_ID_NAME].isin(enhancer_data_frame[constant.GEO_ID_NAME].values) == False]
    missing_data_frame = data_frame.loc[idx_missing]
    missing_data_frame[data_element.variable_name] = constant.NOT_AVAILABLE
    data_frame.drop(idx_missing, inplace=True)

    data_frame_columns = data_frame.columns
    data_frame = data_frame.join(enhancer_data_frame, on=constant.GEO_ID_NAME, rsuffix='_other')
    if type(data_element.source_variable) == str and data_element.source_variable in data_frame.columns:
        data_frame.rename(columns={data_element.source_variable: data_element.variable_name}, inplace=True)

    if data_element.variable_name == 'population_density':
        data_frame.drop(columns=[f"{constant.GEO_ID_NAME}_other"], inplace=True)
        data_frame = join_gazetteer_source(data_frame, data_files, version)

    if data_element.get_strategy in [GetStrategy.CALCULATION, GetStrategy.FILE_AND_CALCULATION]:
        data_frame = get_lambda_calculation(data_frame, data_element.variable_name)

    for column in data_frame.columns:
        if column not in data_frame_columns and column != data_element.variable_name:
            data_frame.drop(columns=[column], inplace=True)

    data_frame = pd.concat([data_frame, non_geocoded_data_frame, missing_data_frame], ignore_index=False)
    data_frame.sort_index(inplace=True)
    return data_frame


def enhance_raster_element(data_frame, data_element, raster_source):
    idx_non_geocoded = data_frame.index[data_frame[constant.GEO_ID_NAME] == constant.ADDRESS_NOT_GEOCODABLE]
    non_geocoded_data_frame = data_frame.loc[idx_non_geocoded]
    non_geocoded_data_frame[data_element.variable_name] = ''
    data_frame.drop(idx_non_geocoded, inplace=True)

    idx_missing = data_frame.index[(data_frame[constant.LATITUDE] == '') | (data_frame[constant.LONGITUDE] == '')]
    missing_data_frame = data_frame.loc[idx_missing]
    missing_data_frame[data_element.variable_name] = constant.NOT_AVAILABLE
    data_frame.drop(idx_missing, inplace=True)

    data_frame[data_element.variable_name] = data_frame.apply(lambda col: calculate_raster_value(
        col[constant.LATITUDE], col[constant.LONGITUDE], raster_source), axis=1)

    data_frame = pd.concat([data_frame, non_geocoded_data_frame, missing_data_frame], ignore_index=False)
    data_frame.sort_index(inplace=True)
    return data_frame


def organize_data_frame_by_source(data_frame, data_sources):
    data_frames = {}
    data_frame.drop(data_frame.index[data_frame[constant.ADDRESS_START_DATE] > data_sources[-1].end_date], inplace=True)
    data_frame.drop(data_frame.index[data_frame[constant.ADDRESS_END_DATE] <= data_sources[0].start_date], inplace=True)
    data_frame.reset_index(drop=True, inplace=True)
    for i, data_source in enumerate(data_sources):
        if i == 0:
            before_time_int = data_frame.index[data_frame[constant.ADDRESS_START_DATE] < data_source.start_date]
            data_frame.loc[before_time_int, constant.ADDRESS_START_DATE] = data_source.start_date
        time_int = data_frame.index[(data_frame[constant.ADDRESS_START_DATE] >= data_source.start_date) & (data_frame[constant.ADDRESS_START_DATE] <= data_source.end_date)]
        new_data_frame = data_frame.loc[time_int].copy()
        after_time_int = new_data_frame.index[new_data_frame[constant.ADDRESS_END_DATE] > data_source.end_date]
        new_data_frame.loc[after_time_int, constant.ADDRESS_END_DATE] = data_source.end_date
        if i + 1 < len(data_sources):
            data_frame.loc[after_time_int, constant.ADDRESS_START_DATE] = data_sources[i + 1].start_date
        new_data_frame.reset_index(drop=True, inplace=True)
        data_frames.update({data_source.start_date: new_data_frame})
    return data_frames


def get_value(data_element, arguments, data_source, version=1):
    if not arguments["fips_concatenated_code"] == constant.ADDRESS_NOT_GEOCODABLE:
        if data_element.get_strategy == GetStrategy.FILE:
            return get_file_value(data_element.source_variable, arguments, data_source.data_frame, data_source.tract_column)
        elif data_element.get_strategy == GetStrategy.FILE_AND_CALCULATION:
            return get_calculated_file_value(data_element.source_variable, arguments, data_source.data_frame,
                                             data_source.tract_column, data_element.variable_name)
        elif data_element.get_strategy == GetStrategy.RASTER_FILE and version == 2 and constant.LATITUDE in arguments.keys() and constant.LONGITUDE in arguments.keys():
            return get_raster_file_value(arguments, data_source)
        else:
            return None
    else:
        return None


def get_acs_data_frame_value(data_frame, data_element, arguments, data_files, data_year):
    if not arguments["fips_concatenated_code"] == constant.ADDRESS_NOT_GEOCODABLE:
        if "," not in data_element.source_variable and data_element.source_variable not in data_frame.columns:
            return constant.NOT_AVAILABLE
        elif arguments["fips_concatenated_code"] not in data_frame[constant.GEO_ID_NAME]:
            return constant.NOT_AVAILABLE
        elif data_element.get_strategy == GetStrategy.CALCULATION:
            # Some calculated values require values from two sources, calculation func will accept two values in a list
            if "," in data_element.source_variable:
                source_var = data_element.source_variable[:data_element.source_variable.index(',')]
                calc_var = data_element.source_variable[data_element.source_variable.index(',') + 1:]
                return get_acs_calculation(data_element.variable_name,
                                                     [data_frame.loc[arguments["fips_concatenated_code"], source_var],
                                                      data_frame.loc[arguments["fips_concatenated_code"], calc_var]],
                                                      arguments, data_files, data_year)
            else:
                return get_acs_calculation(data_element.variable_name,
                                           data_frame.loc[arguments["fips_concatenated_code"], data_element.source_variable],
                                            arguments, data_files, data_year)
        else:
            return data_frame.loc[arguments["fips_concatenated_code"], data_element.source_variable]
    else:
        return None


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
    arguments = {
        "host_name": "https://api.census.gov/data",
        "data_year": "2018",
        "dataset_name": get_acs_dataset_name(source_variable),
        "variables": source_variable,
        "geographies": construct_geography_argument(arguments),
        "key": os.getenv("census_api_key")
    }

    census_api_interpolation_string = "{host_name}/{data_year}/{dataset_name}?get={variables}&{geographies}" \
                                      "&key={key}"
    api_url = api.construct_url(census_api_interpolation_string, arguments)
    return api.get_value(api_url)


def get_acs_values(data_set, source_variables, arguments, test_mode=False):
    # The vintage year (e.g., V2019) refers to the final year of the time series.
    # The reference date for all estimates is July 1, unless otherwise specified.
    arguments = {
        "host_name": "https://api.census.gov/data",
        "data_year": "2018",
        "dataset_name": data_set,
        "variables": source_variables,
        "geographies": construct_geography_argument(arguments),
        "key": os.getenv("census_api_key")
    }
    if not test_mode:
        census_api_interpolation_string = "{host_name}/{data_year}/{dataset_name}?get={variables}&{geographies}" \
                                          "&key={key}"
    else:
        census_api_interpolation_string = "{host_name}/{data_year}/{dataset_name}?get={variables}&{geographies}"

    api_url = api.construct_url(census_api_interpolation_string, arguments)
    return api.get_values(api_url, test_mode)


def get_acs_batch(data_set, source_variables, geographies, data_year="2018", test_mode=False):
    arguments = {
        "host_name": "https://api.census.gov/data",
        "data_year": data_year,
        "dataset_name": data_set,
        "variables": source_variables,
        "geographies": geographies,
        "key": os.getenv("census_api_key")
    }
    if not test_mode:
        census_api_interpolation_string = "{host_name}/{data_year}/{dataset_name}?get={variables}&{geographies}" \
                                          "&key={key}"
    else:
        census_api_interpolation_string = "{host_name}/{data_year}/{dataset_name}?get={variables}&{geographies}"

    api_url = api.construct_url(census_api_interpolation_string, arguments)
    return api.get_batch_values(api_url, test_mode)


def get_acs_calculation(variable_name, source_value, arguments, data_files, data_year):
    # TODO: Change from using hardcoded variable_name checks
    if source_value == constant.NOT_AVAILABLE:
        return constant.NOT_AVAILABLE
    elif variable_name in ["percent_below_200_of_fed_poverty_level", "percent_below_300_of_fed_poverty_level"]:
        return str(round(100 * float(source_value[0]) / float(source_value[1]), 2))
    elif variable_name == 'housing_percent_occupied_units_lacking_plumbing':
        return str(100 - float(source_value))
    elif variable_name == 'housing_percent_occupied_lacking_complete_kitchen':
        return str(100 - float(source_value))
    elif variable_name == 'population_density':
        # The Gazetteer data source is only used in this calculated value, so its time range is found with this func
        gazetteer_index = data_source_intersection(data_files[SedohDataSource.Gazetteer], data_year)
        aland = get_file_value("ALAND", arguments, data_files[SedohDataSource.Gazetteer][gazetteer_index].data_frame,
                               data_files[SedohDataSource.Gazetteer][gazetteer_index].tract_column)
        if aland == constant.NOT_AVAILABLE or int(aland) == 0:
            return constant.NOT_AVAILABLE
        else:
            return str(round(1000000 * (float(source_value) / int(aland)), 0))
    else:
        return None


def data_source_intersection(data_sources, data_year):
    for index, source in enumerate(data_sources):
        if source.start_date <= data_year <= source.end_date:
            return index
    return 0


# File specific methods
def get_file_value(source_variable, arguments, data_file, data_file_search_column_name):
    # TODO: Fix the naming and the intent of this function
    indexes = data_file.index[data_file[data_file_search_column_name] == arguments["fips_concatenated_code"]].tolist()
    if len(indexes) == 0:
        return constant.NOT_AVAILABLE
    elif len(indexes) == 1:
        return data_file.iloc[indexes[0]][source_variable]
    else:
        return "Error"


def get_calculated_file_value(source_variables, arguments, data_file, data_file_search_column_name, variable_name):
    source_values = {}

    for source_variable in source_variables:
        indexes = data_file.index[
            data_file[data_file_search_column_name] == arguments["fips_concatenated_code"]].tolist()

        if len(indexes) == 0:
            # The data file could not find the fips_concatenated_code,
            # possibly because the data file does not cover the census tract
            return constant.NOT_AVAILABLE
        else:
            source_values[source_variable] = data_file.iloc[indexes[0]][source_variable]
    # TODO: Refactor these condition based calculations
    if variable_name == 'food_fraction_of_population_with_low_access':
        if source_values['Urban'] == '1':
            return str(float(source_values['lapop1share']) * 100)
        else:
            return str(source_values['lapop10share'])

    return None


def get_raster_file_value(arguments, data_file):
    latitude = round(float(arguments[constant.LATITUDE]), data_file.precision)
    longitude = round(float(arguments[constant.LONGITUDE]), data_file.precision)
    if data_file.latitude_range[0] <= latitude <= data_file.latitude_range[1] and data_file.longitude_range[
        0] <= longitude <= data_file.longitude_range[1]:
        latitude_difference = round(latitude - data_file.latitude_range[0], data_file.precision)
        longitude_difference = round(longitude - data_file.longitude_range[0], data_file.precision)
        latitude_index = data_file.latitude_transform - int(round(latitude_difference / data_file.step, 2))
        longitude_index = int(round(longitude_difference / data_file.step, 2))
        value = data_file.array[latitude_index][longitude_index]
        if value != -99:
            return value
    return constant.NOT_AVAILABLE
