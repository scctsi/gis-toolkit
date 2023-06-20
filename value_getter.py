import os
import pandas as pd
import api
from dotenv import load_dotenv
import math
import sedoh_data_structure as sds
from data_structure import GetStrategy
import constant

load_dotenv()


# Note on naming scheme: parameters = named variables passed to a function,
#                        arguments = expressions used when calling the function,
#                        options = optional arguments which allow yoy to customize the function


def join_gazetteer_source(data_frame, data_files, version):
    gazetteer_index = -1
    if version == 'comprehensive':
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
        if math.isnan(float(la_pop_one)):
            return constant.NOT_AVAILABLE
        else:
            return str(float(la_pop_one) * 100)
    else:
        if math.isnan(float(la_pop_ten)):
            return constant.NOT_AVAILABLE
        else:
            return la_pop_ten


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
            return str(value)
    return constant.NOT_AVAILABLE


def get_raster_value(src, lon, lat):
    py, px = src.index(float(lon), float(lat))
    try:
        return str(src.read(1)[py][px])
    except IndexError:
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


def enhance_data_element(data_frame, enhancer_data_frame, data_element, data_files, version):
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

    if sds.SedohDataSource.SCEHSC in data_element.data_source:
        data_frame[data_element.variable_name] = data_frame.apply(lambda col: calculate_raster_value(
            col[constant.LATITUDE], col[constant.LONGITUDE], raster_source), axis=1)
    else:
        nasa_source = raster_source.read()
        data_frame[data_element.variable_name] = data_frame.apply(lambda col: get_raster_value(
            nasa_source, col[constant.LONGITUDE], col[constant.LATITUDE]), axis=1)
        raster_source.close()

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


def get_acs_dataset_name(source_variable):
    if source_variable[0].lower() == 's':
        return 'acs/acs5/subject'
    elif source_variable[0:2].lower() == 'dp':
        return 'acs/acs5/profile'
    else:
        return "acs/acs5"


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

