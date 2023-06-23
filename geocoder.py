# Test data: https://github.com/EthanRBrown/rrad which is taken from https://openaddresses.io/
from datetime import datetime, timedelta
from config import input_config, database_config
import requests
from enum import Enum
import api
from dotenv import load_dotenv
import os
import sys
from io import StringIO
import pandas as pd
import json
from progress_bar import progress
import constant
from address import Address, Coordinate
import importer

load_dotenv()

BENCHMARK = "Public_AR_Census2020"
VINTAGE = "Census2020_Census2020"


class Decade(Enum):
    Ten = 1  # 2010-2019
    Twenty = 2  # 2020-2029


# Decade 'Twenty' extends from 2021-present, so the vintage value needs to be updated yearly
decade_dict = {
    Decade.Ten: {
        "Benchmark": "Public_AR_Current",
        "Vintage": "Census2010_Current",
        "start_date": datetime(2010, 1, 1),
        "end_date": datetime(2019, 12, 31)},
    Decade.Twenty: {
        "Benchmark": "Public_AR_Current",
        "Vintage": "Census2020_Current",
        "start_date": datetime(2020, 1, 1),
        "end_date": datetime(2029, 12, 31)
    }
}


def address_fields_present(data_frame):
    if (input_config["street"] in data_frame.columns and
            input_config["city"] in data_frame.columns and
            input_config["state"] in data_frame.columns and
            input_config["zip"] in data_frame.columns):
        return True
    else:
        return False


def coordinate_fields_present(data_frame):
    if (input_config["latitude"] in data_frame.columns and
            input_config["longitude"] in data_frame.columns):
        return True
    else:
        return False


def geocodable(data_frame):
    if address_fields_present(data_frame):
        return True
    else:
        return False


def modify_column_names_to_default(data_frame):
    for default, custom in input_config.items():
        if custom in data_frame.columns:
            data_frame.rename(columns={custom: default}, inplace=True)
    return data_frame


def modify_column_names_to_custom(data_frame):
    for default, custom in input_config.items():
        if default in data_frame.columns:
            data_frame.rename(columns={default: custom}, inplace=True)
    return data_frame


def write_to_geocoded_cache(data_frame):
    import sqlite3
    conn = sqlite3.connect(database_config)
    data_frame = modify_column_names_to_default(data_frame)
    geo_cache_columns = ["address_id", "geo_id_name", "latitude", "longitude", "decade"]
    data_frame.drop(columns=[column for column in data_frame.columns if column not in geo_cache_columns], inplace=True)
    data_frame.to_sql(name="geo_cache", con=conn, if_exists="append", index=False)


def get_geocoded_cache():
    import sqlite3
    conn = sqlite3.connect(database_config)
    c = conn.cursor()
    c.execute(f'''
              CREATE TABLE IF NOT EXISTS geo_cache
              ([address_id] TEXT, [geo_id_name] TEXT, [latitude] TEXT, [longitude] TEXT, [decade] TEXT)
              ''')
    conn.commit()
    return pd.read_sql_query("SELECT * FROM geo_cache", conn)


def geocode_data_frame_from_cache(data_frame):
    cache_data_frame = get_geocoded_cache()
    index = data_frame.index
    geocoded_data_frame = modify_column_names_to_default(data_frame)
    geocoded_data_frame = geocoded_data_frame.merge(cache_data_frame, on=["address_id", "decade"], suffixes=['_from_input', '_from_cache'])
    geocoded_data_frame = modify_column_names_to_custom(geocoded_data_frame)
    geocoded_data_frame = check_unnamed(geocoded_data_frame)
    geocoded_data_frame.set_index(index, inplace=True)
    return geocoded_data_frame


def filter_data_frame_with_geocoded_cache(data_frame):
    cache_data_frame = get_geocoded_cache()
    idx_in_cache = data_frame.index[
        (data_frame[input_config["address_id"]].isin(cache_data_frame["address_id"].values) == True) &
        (data_frame[constant.DECADE].isin(cache_data_frame[constant.DECADE].values) == True)]
    geocoded_data_frame = data_frame.loc[idx_in_cache]

    index = geocoded_data_frame.index
    geocoded_data_frame = modify_column_names_to_default(geocoded_data_frame)
    geocoded_data_frame = geocoded_data_frame.merge(cache_data_frame, on=["address_id", "decade"], suffixes=['_from_input', '_from_cache'])
    geocoded_data_frame = modify_column_names_to_custom(geocoded_data_frame)
    geocoded_data_frame = check_unnamed(geocoded_data_frame)
    geocoded_data_frame.set_index(index, inplace=True)

    data_frame.drop(idx_in_cache, inplace=True)
    data_frame = check_unnamed(data_frame)
    return data_frame, geocoded_data_frame


def geocode_address_in_data_frame(data_frame):
    # TODO: Rewrite using pandas.DataFrame.apply()
    data_frame[input_config["geo_id_name"]] = ''

    for index, row in data_frame.iterrows():
        data_frame.iloc[index][input_config["geo_id_name"]] = geocode_address_to_census_tract(
            Address(row[input_config["street"]], row[input_config["city"]], input_config["state"], row[input_config["zip"]]))

    return data_frame


def check_unnamed(data_frame):
    data_frame.drop(columns=[column for column in data_frame.columns if "Unnamed" in column], inplace=True)
    return data_frame


def check_decade(data_frame):
    data_frame.drop(columns=[column for column in data_frame.columns if column == "decade"], inplace=True)
    return data_frame


def geocode_data_frame(input_data_frame, version):
    if version == "latest":
        input_data_frame[constant.DECADE] = str(Decade.Twenty.value)
    elif version == "comprehensive":
        input_data_frame = arrange_data_frame_by_decade(input_data_frame.copy())

    data_frame, geocoded_cache_data_frame = filter_data_frame_with_geocoded_cache(input_data_frame.copy())

    print(f'{len(geocoded_cache_data_frame)} addresses geocoded from cache ({round(len(geocoded_cache_data_frame)/len(input_data_frame), 2) * 100}%)')

    index = data_frame.index
    data_frame.reset_index(drop=True, inplace=True)

    if coordinate_fields_present(data_frame) and address_fields_present(data_frame):
        coordinate_missing = data_frame.index[(data_frame[input_config["latitude"]] == '') | (data_frame[input_config["longitude"]] == '')]
        coordinate_data_frame = data_frame.drop(coordinate_missing)
        address_data_frame = data_frame.loc[coordinate_missing]
        coordinate_data_frame = geocode_coordinates_in_data_frame(coordinate_data_frame, version)
        address_data_frame = geocode_addresses_in_data_frame(address_data_frame, version)
        data_frame = pd.concat([coordinate_data_frame, address_data_frame], ignore_index=True)
    elif coordinate_fields_present(data_frame):
        data_frame = geocode_coordinates_in_data_frame(data_frame, version)
    elif address_fields_present(data_frame):
        data_frame = geocode_addresses_in_data_frame(data_frame, version)

    data_frame.set_index(index, inplace=True)
    data_frame = pd.concat([geocoded_cache_data_frame, data_frame], ignore_index=False)
    data_frame.sort_index(inplace=True)
    data_frame = check_decade(data_frame)
    return data_frame


def geocode_coordinates_in_data_frame(data_frame, version):
    if version == 'latest':
        coordinates_to_geocoder(data_frame, decade_dict[Decade.Twenty])
        data_frame = geocode_data_frame_from_cache(data_frame)
    if version == 'comprehensive':
        # Addresses are first re-organized by decade as census tract geography is updated after each census
        data_frames = separate_arranged_data_frame(data_frame)
        for decade in Decade:
            if len(data_frames[decade.name]) > 0:
                # Addresses from a given decade are geocoded with a "vintage" and "benchmark" specific to the geography of that decade
                coordinates_to_geocoder(data_frames[decade.name], decade_dict[decade])
                data_frames[decade.name] = geocode_data_frame_from_cache(data_frames[decade.name])
        data_frame = pd.concat(list(data_frames.values()), ignore_index=True)
    data_frame = check_unnamed(data_frame)
    return data_frame


def parse_batch_coordinates(data_frame):
    data_frame["latitude"] = ''
    data_frame["longitude"] = ''
    for row in data_frame.itertuples():
        if getattr(row, 'census_tract') != constant.ADDRESS_NOT_GEOCODABLE:
            comma = getattr(row, 'latitude_longitude').index(',')
            data_frame.loc[row.Index, "longitude"] = getattr(row, 'latitude_longitude')[0: comma]
            data_frame.loc[row.Index, "latitude"] = getattr(row, 'latitude_longitude')[comma + 1:]
    return data_frame


def geocode_addresses_in_data_frame(data_frame, version):
    """
    :param data_frame: Data frame of addresses, to be geocoded
    :param data_key: Key of save file, associated with a file's geocoding process
    :param version: Toggles geocoding in one time frame or multiple (decades)
    :return: Data frame with new "SPATIAL_GEOID" column, to be enhanced
    """
    if version == 'latest':
        addresses_to_geocoder(data_frame, decade_dict[Decade.Twenty])
        data_frame = geocode_data_frame_from_cache(data_frame)
    if version == 'comprehensive':
        # Addresses are first re-organized by decade as census tract geography is updated after each census
        data_frames = separate_arranged_data_frame(data_frame)
        for decade in Decade:
            if len(data_frames[decade.name]) > 0:
                # Addresses from a given decade are geocoded with a "vintage" and "benchmark" specific to the geography
                # of that decade
                addresses_to_geocoder(data_frames[decade.name], decade_dict[decade])
                data_frames[decade.name] = geocode_data_frame_from_cache(data_frames[decade.name])
        data_frame = pd.concat(list(data_frames.values()), ignore_index=True)
    data_frame = check_unnamed(data_frame)
    return data_frame


def coordinates_to_geocoder(data_frame, decade):
    try:
        geocode_coordinates_to_census_tract(data_frame, decade)
    except Exception as e:
        raise Exception(e)


def addresses_to_geocoder(data_frame, decade):
    """
    :param data_frame: Data frame of addresses, to be geocoded
    :param data_key: Key of save file, associated with a file's geocoding process
    :param decade: Sets vintage and benchmark values of geocoder
    :return:
    """
    addresses = []
    for row in data_frame.itertuples():
        addresses.append(Address(getattr(row, input_config["street"]), getattr(row, input_config["city"]), getattr(row, input_config["state"]), getattr(row, input_config["zip"])))
    try:
        geocode_addresses_to_census_tract(data_frame, addresses, decade)
    except Exception as e:
        raise Exception(e)


def separate_arranged_data_frame(data_frame):
    data_frames = {}
    for decade in Decade:
        idx_matched_decade = data_frame.index[data_frame[constant.DECADE] == str(decade.value)]
        matched_decade_data_frame = data_frame.loc[idx_matched_decade]
        matched_decade_data_frame.reset_index(drop=True, inplace=True)
        data_frames.update({decade.name: matched_decade_data_frame})
    return data_frames


def arrange_data_frame_by_decade(data_frame):
    data_frames = []
    # Decades are defined as starting on the first year after new census information is released (2001, 2011, etc)
    # Addresses occurring entirely before the range of valid census information (2001-present) are cropped from data
    # Addresses only starting before this range have their start date redefined to the start of this range (2001)
    decades = list(decade_dict.keys())
    data_frame.drop(data_frame.index[data_frame[input_config["address_start_date"]] > decade_dict[decades[-1]]["end_date"]],
                    inplace=True)
    data_frame.drop(data_frame.index[data_frame[input_config["address_end_date"]] <= decade_dict[decades[0]]["start_date"]],
                    inplace=True)
    data_frame.reset_index(drop=True, inplace=True)

    before_time_int = data_frame.index[data_frame[input_config["address_start_date"]] < decade_dict[decades[0]]["start_date"]]
    data_frame.loc[before_time_int, input_config["address_start_date"]] = decade_dict[decades[0]]["start_date"]
    for decade in Decade:
        # Addresses starting within a decade are found and copied. If they extend into the next decade, a new instance
        # of the address is created with appropriate start and end dates
        idx_decade = data_frame.index[(data_frame[input_config["address_start_date"]] >= decade_dict[decade]["start_date"]) & (
                data_frame[input_config["address_start_date"]] <= decade_dict[decade]["end_date"])]
        decade_data_frame = data_frame.loc[idx_decade].copy()
        decade_remainder = decade_data_frame.index[
            decade_data_frame[input_config["address_end_date"]] > decade_dict[decade]["end_date"]]
        data_frame.loc[decade_remainder, input_config["address_start_date"]] = decade_dict[decade]["end_date"] + timedelta(days=1)
        decade_data_frame.loc[decade_remainder, input_config["address_end_date"]] = decade_dict[decade]["end_date"]
        decade_data_frame.reset_index(drop=True, inplace=True)
        decade_data_frame[constant.DECADE] = str(decade.value)
        data_frames.append(decade_data_frame)
    data_frame = pd.concat(data_frames, ignore_index=True)
    return data_frame


def check_temp_dir():
    if not os.path.isdir('./temp'):
        os.mkdir('./temp')
    return None


def geocode_coordinates_to_census_tract(data_frame, decade):
    # If there is a remainder, another batch is added to accommodate those addresses
    api_url = "https://geocoding.geo.census.gov/geocoder/geographies/coordinates"
    payload = {'benchmark': decade['Benchmark'], 'vintage': decade['Vintage']}
    for i, row in data_frame.iterrows():
        payload.update({'x': row[input_config["longitude"]], 'y': row[input_config["latitude"]]})
        try:
            response = requests.post(api_url, data=payload)
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)
        res = json.loads(response.text)
        try:
            data_frame.loc[i, input_config["geo_id_name"]] = res['result']['geographies']['Census Tracts'][0]["GEOID"]
        except KeyError:
            data_frame.loc[i, input_config["geo_id_name"]] = constant.ADDRESS_NOT_GEOCODABLE
        write_to_geocoded_cache(data_frame.loc[i: i])
        if i + 1 == len(data_frame):
            progress(i + 1, len(data_frame), status='Geocoding complete')
        else:
            progress(i + 1, len(data_frame), status='Geocoding in progress')
    return None


def geocode_addresses_to_census_tract(data_frame, addresses, decade, batch_limit=10000):
    """
    Batch geocodes more than 10,000 addresses

    :param addresses: List of addresses of type Address
    :param data_key: Key of save file, associated with a file's geocoding process
    :param decade: Sets vintage and benchmark values of geocoder
    :param batch_limit: Default is census geocoder's maximum batch size
    :return: None
    """
    check_temp_dir()
    batch_calls = int(len(addresses) / batch_limit)
    # If there is a remainder, another batch is added to accommodate those addresses
    if len(addresses) % batch_limit != 0:
        batch_calls += 1
    api_url = "https://geocoding.geo.census.gov/geocoder/geographies/addressbatch"
    payload = {'benchmark': decade['Benchmark'], 'vintage': decade['Vintage']}
    column_names = ["address_index", "input_address", "match_indicator", "match_type", "output_address",
                    "latitude_longitude", "line_id", "line_id_side",
                    "state_code", "county_code", "tract_code", "block_code"]
    # Each batch needs its own API request
    for i in range(batch_calls):
        # The end of addresses can only be read on the last batch
        if i + 1 == batch_calls:
            address_batch_data_frame = Address.to_data_frame(addresses[i * batch_limit:])
        else:
            address_batch_data_frame = Address.to_data_frame(addresses[i * batch_limit:(i + 1) * batch_limit])
        address_batch_data_frame.to_csv('./temp/addresses.csv', header=False, index=True)
        files = {'addressFile': ('addresses.csv', open('./temp/addresses.csv', 'rb'), 'text/csv')}
        try:
            response = requests.post(api_url, files=files, data=payload, verify=False)
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)
        # Geocoded address can be returned in a different order, the following lines correct their indexes and sort them
        geocoded_address_batch_data_frame = pd.read_csv(StringIO(response.text), sep=",", names=column_names,
                                                        dtype='str')
        geocoded_address_batch_data_frame.index = geocoded_address_batch_data_frame['address_index'].astype(int).add(
            i * batch_limit)
        geocoded_address_batch_data_frame['address_index'] = geocoded_address_batch_data_frame.index
        geocoded_address_batch_data_frame = geocoded_address_batch_data_frame.sort_index()
        # The values created in 'census tract' are the "SPATIAL_GEOID" used in the enhancement process
        geocoded_address_batch_data_frame['census_tract'] = geocoded_address_batch_data_frame['state_code'] + \
                                                            geocoded_address_batch_data_frame['county_code'] + \
                                                            geocoded_address_batch_data_frame['tract_code']
        non_matched_addresses = geocoded_address_batch_data_frame.index[
            (geocoded_address_batch_data_frame['match_indicator'] == 'No_Match') | (
                    geocoded_address_batch_data_frame['match_indicator'] == 'Tie')]
        geocoded_address_batch_data_frame.loc[non_matched_addresses, 'census_tract'] = constant.ADDRESS_NOT_GEOCODABLE
        geocoded_data_frame = parse_batch_coordinates(geocoded_address_batch_data_frame)
        if i + 1 == batch_calls:
            batch_data_frame = data_frame.loc[i * batch_limit:]
            progress((i + 1) * batch_limit, batch_calls * batch_limit, status='Geocoding complete')
        else:
            batch_data_frame = data_frame.loc[i * batch_limit: (i + 1) * batch_limit - 1]
            progress((i + 1) * batch_limit, batch_calls * batch_limit, status='Geocoding in progress')
        batch_data_frame["geo_id_name"] = geocoded_data_frame["census_tract"]
        batch_data_frame["latitude"] = geocoded_data_frame["latitude"]
        batch_data_frame["longitude"] = geocoded_data_frame["longitude"]
        write_to_geocoded_cache(batch_data_frame)
    return None


def geocode_coordinate_to_census_tract(coordinate):
    arguments = {
        "host_name": "https://geocoding.geo.census.gov",
        "latitude": coordinate.latitude,
        "longitude": coordinate.longitude,
        "benchmark": BENCHMARK,
        "vintage": VINTAGE,
        "format": "json",
    }

    geocoder_interpolation_string = "{host_name}/geocoder/geographies/coordinates?" \
                                    "x={longitude}&y={latitude}" \
                                    "&benchmark={benchmark}&vintage={vintage}&format={format}"

    api_url = api.construct_url(geocoder_interpolation_string, arguments)
    response = api.get_response(api_url, test_mode=True)
    # census_tract_information = response["result"]["addressMatches"][0]["geographies"]["Census Tracts"][0]
    # return census_tract_information["STATE"] + census_tract_information["COUNTY"] + census_tract_information["TRACT"]
    return response


def geocode_address_to_census_tract(address):
    arguments = {
        "host_name": "https://geocoding.geo.census.gov",
        "street": address.street,
        "city": address.city,
        "state": address.state,
        "zip": address.zip,
        "benchmark": BENCHMARK,
        "vintage": VINTAGE,
        "format": "json",
        "key": os.getenv("census_api_key")
    }

    geocoder_interpolation_string = "{host_name}/geocoder/geographies/address?" \
                                    "street={street}&city={city}&state={state}" \
                                    "&benchmark={benchmark}&vintage={vintage}&format={format}"

    api_url = api.construct_url(geocoder_interpolation_string, arguments)
    response = api.get_response(api_url)

    # TODO: Raise error if response returns multiple address matches
    census_tract_information = response["result"]["addressMatches"][0]["geographies"]["Census Tracts"][0]
    return census_tract_information["STATE"] + census_tract_information["COUNTY"] + census_tract_information["TRACT"]

# address_1 = Address('1745 T Street Southeast', 'Washington', 'DC', '20020')
# address_2 = Address('6007 Applegate Lane', 'Louisville', 'KY', '40219')
# address_3 = Address('560 Penstock Drive', 'Grass Valley', 'CA', '95945')
# address_4 = Address('150 Carter Street', 'Manchester', 'CT', '06040')
# address_5 = Address('2721 Lindsay Avenue', 'Louisville', 'KY', '20022')
#
# geocode_addresses_to_census_tract([address_1, address_2, address_3, address_4, address_5])
