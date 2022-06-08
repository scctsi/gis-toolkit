# Test data: https://github.com/EthanRBrown/rrad which is taken from https://openaddresses.io/
from datetime import datetime

import requests
from enum import Enum
import api
from dotenv import load_dotenv
import os
import sys
from io import StringIO
import pandas as pd
import json

import constant
from address import Address
import importer

load_dotenv()

BENCHMARK = "Public_AR_Census2020"
VINTAGE = "Census2020_Census2020"


class Decade(Enum):
    Zero = 0  # 2000-2010
    Ten = 1  # 2010-2020
    Twenty = 2  # 2020-2030


# Decade 'Twenty' extends from 2020-present, so the vintage value needs to be updated yearly

decade_dict = {
    Decade.Zero: {
        "Benchmark": "Public_AR_Current",
        "Vintage": "Census2010_Current"},
    Decade.Ten: {
        "Benchmark": "Public_AR_Current",
        "Vintage": "Census2020_Current"},
    Decade.Twenty: {
        "Benchmark": "Public_AR_Current",
        "Vintage": "ACS2021_Current"}
}


def address_fields_present(data_frame):
    if ('street' in data_frame and
            'city' in data_frame and
            'state' in data_frame and
            'zip' in data_frame and
            'SPATIAL_GEOID' not in data_frame):
        return True
    else:
        return False


def geocodable(data_frame):
    if address_fields_present(data_frame):
        return True
    else:
        return False


def geocode_data_frame(data_frame):
    # TODO: Rewrite using pandas.DataFrame.apply()
    data_frame[constant.GEO_ID_NAME] = ''

    for index, row in data_frame.iterrows():
        data_frame.iloc[index][constant.GEO_ID_NAME] = geocode_address_to_census_tract(
            Address(row['street'], row['city'], row['state'], row['zip']))

    return data_frame


def parse_lat_long(data_frame, geocoded_data_frame):
    data_frame['latitude'] = ''
    data_frame['longitude'] = ''
    for index, row in geocoded_data_frame.iterrows():
        if row['census_tract'] != constant.ADDRESS_NOT_GEOCODABLE:
            comma = row['latitude_longitude'].index(',')
            data_frame.loc[index, 'longitude'] = row['latitude_longitude'][0: comma]
            data_frame.loc[index, 'latitude'] = row['latitude_longitude'][comma + 1:]
    return data_frame


def geocode_addresses_in_data_frame(data_frame, data_key, version=1):
    """
    :param data_frame: Data frame of addresses, to be geocoded
    :param data_key: Key of save file, associated with a file's geocoding process
    :param version: Toggles geocoding in one time frame or multiple (decades)
    :return: Data frame with new "SPATIAL_GEOID" column, to be enhanced
    """
    if version is None or version == 1:
        data_frame[constant.GEO_ID_NAME] = ''
        addresses_to_geocoder(data_frame, data_key, decade_dict[Decade.Ten])
        geocoded_data_frame = importer.import_file(f"./temp/geocoded_{data_key}.csv")
        data_frame[constant.GEO_ID_NAME] = geocoded_data_frame['census_tract']
    if version == 2:
        data_frames = separate_data_frame_by_decade(data_frame)
        for decade in Decade:
            if len(data_frames[decade.name]) > 0:
                data_frames[decade.name][constant.GEO_ID_NAME] = ''
                addresses_to_geocoder(data_frames[decade.name], f"{data_key}_{decade.name}", decade_dict[decade])
                geocoded_data_frame = importer.import_file(f"./temp/geocoded_{data_key}_{decade.name}.csv")
                data_frames[decade.name][constant.GEO_ID_NAME] = geocoded_data_frame['census_tract']
                data_frames[decade.name] = parse_lat_long(data_frames[decade.name], geocoded_data_frame)
        data_frame = pd.concat(list(data_frames.values()), ignore_index=True)
        data_frame.drop(columns=['Unnamed: 0'], inplace=True)
    return data_frame


def addresses_to_geocoder(data_frame, data_key, decade):
    """
    :param data_frame: Data frame of addresses, to be geocoded
    :param data_key: Key of save file, associated with a file's geocoding process
    :param decade: Sets vintage and benchmark values of geocoder
    :return:
    """
    data_frame[constant.GEO_ID_NAME] = ''
    addresses = []
    for row in data_frame.itertuples():
        addresses.append(Address(row.street, row.city, row.state, row.zip))
    try:
        geocode_addresses_to_census_tract(addresses, data_key, decade)
    except Exception as e:
        raise Exception(e)


def separate_data_frame_by_decade(data_frame):
    data_frames = {}
    decades = [datetime(2000, 1, 1), datetime(2010, 1, 1), datetime(2020, 1, 1), datetime(2030, 1, 1)]
    data_frame.drop(data_frame.index[data_frame[constant.ADDRESS_END_DATE] <= decades[0]], inplace=True)
    before_first_decade = data_frame.index[data_frame[constant.ADDRESS_START_DATE] < decades[0]]
    data_frame.loc[before_first_decade, constant.ADDRESS_START_DATE] = decades[0]
    for i in range(3):
        decade = data_frame.index[(decades[i] <= data_frame[constant.ADDRESS_START_DATE]) & (
                    data_frame[constant.ADDRESS_START_DATE] < decades[i + 1])]
        decade_data_frame = data_frame.loc[decade].copy()
        decade_remainder = decade_data_frame.index[decade_data_frame[constant.ADDRESS_END_DATE] >= decades[i + 1]]
        data_frame.loc[decade_remainder, constant.ADDRESS_START_DATE] = decades[i + 1]
        decade_data_frame.loc[decade_remainder, constant.ADDRESS_END_DATE] = decades[i + 1]
        decade_data_frame.reset_index(drop=True, inplace=True)
        data_frames.update({Decade(i).name: decade_data_frame})
    return data_frames


def check_temp_dir():
    if not os.path.isdir('./temp'):
        os.mkdir('./temp')
    return None


def check_save_file():
    check_temp_dir()
    if not os.path.exists('temp/geocoder_save_file.json'):
        with open('temp/geocoder_save_file.json', "w") as save_file:
            json.dump({}, save_file)
    return None


def save_geocode_progress(data_key, batch_index, status="Incomplete", error_message=""):
    """
    Saves the geocoding progress in case an error disrupts geocoding

    :param data_key: Key of save file, associated with a file's geocoding process
    :param batch_index: Progress of the geocoding process
    :param status: Complete/Incomplete
    :param error_message: Request exception message
    :return: None
    """
    check_save_file()
    with open('temp/geocoder_save_file.json', "r+") as save_file:
        data = json.load(save_file)
        if data_key not in data.keys():
            data[data_key] = {'last_successful_batch': batch_index, "status": "Incomplete", "error_message": ""}
        else:
            data[data_key]['last_successful_batch'] = batch_index
            data[data_key]['status'] = status
            data[data_key]['error_message'] = error_message
        save_file.seek(0)
        json.dump(data, save_file, indent=4)
        save_file.truncate()
    return None


def load_geocode_progress(data_key):
    """
    :param data_key: Key of save file, associated with a file's geocoding process
    :return: Batch index of batch geocoding progress
    """
    check_save_file()
    with open('temp/geocoder_save_file.json') as save_file:
        data = json.load(save_file)
        if data_key in data.keys():
            batch_index = data[data_key]['last_successful_batch']
        else:
            batch_index = 0
            save_geocode_progress(data_key, batch_index)
    return batch_index


def geocode_addresses_to_census_tract(addresses, data_key, decade, batch_limit=10000):
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
    column_names = ["address_id", "input_address", "match_indicator", "match_type", "output_address",
                    "latitude_longitude", "line_id", "line_id_side",
                    "state_code", "county_code", "tract_code", "block_code"]
    # Loading the progress of a key-specific batch geocoding process
    batch_progress = load_geocode_progress(data_key)
    # Each batch needs it's own API request
    for i in range(batch_progress, batch_calls):
        # The end of addresses can only be read on the last batch
        if i + 1 == batch_calls:
            address_batch_data_frame = Address.to_data_frame(addresses[i * batch_limit:])
        else:
            address_batch_data_frame = Address.to_data_frame(addresses[i * batch_limit:(i + 1) * batch_limit])
        address_batch_data_frame.to_csv('./temp/addresses.csv', header=False, index=True)
        files = {'addressFile': ('addresses.csv', open('./temp/addresses.csv', 'rb'), 'text/csv')}
        try:
            response = requests.post(api_url, files=files, data=payload)
        except requests.exceptions.RequestException as e:
            save_geocode_progress(data_key, i, "Incomplete", str(e))
            raise SystemExit(e)
        # Geocoded address can be returned in a different order, the following lines correct their indexes and sort them
        geocoded_address_batch_data_frame = pd.read_csv(StringIO(response.text), sep=",", names=column_names,
                                                        dtype='str')
        geocoded_address_batch_data_frame.index = geocoded_address_batch_data_frame['address_id'].astype(int).add(
            i * batch_limit)
        geocoded_address_batch_data_frame['address_id'] = geocoded_address_batch_data_frame.index
        geocoded_address_batch_data_frame = geocoded_address_batch_data_frame.sort_index()
        # The values created in 'census tract' are the "SPATIAL_GEOID" used in the enhancement process
        geocoded_address_batch_data_frame['census_tract'] = geocoded_address_batch_data_frame['state_code'] + \
                                                            geocoded_address_batch_data_frame['county_code'] + \
                                                            geocoded_address_batch_data_frame['tract_code']
        non_matched_addresses = geocoded_address_batch_data_frame.index[
            (geocoded_address_batch_data_frame['match_indicator'] == 'No_Match') | (
                    geocoded_address_batch_data_frame['match_indicator'] == 'Tie')]
        geocoded_address_batch_data_frame.loc[non_matched_addresses, 'census_tract'] = constant.ADDRESS_NOT_GEOCODABLE
        if i == 0:
            geocoded_address_batch_data_frame.to_csv(f"./temp/geocoded_{data_key}.csv")
        else:
            geocoded_address_batch_data_frame.to_csv(f"./temp/geocoded_{data_key}.csv", header=False, mode='a')
        # The index of the next batch to be geocoded is saved and once the loop ends, the completion status is saved
        new_batch_index = i + 1
        save_geocode_progress(data_key, new_batch_index)
    save_geocode_progress(data_key, load_geocode_progress(data_key), "Complete")
    pd.set_option('display.max_columns', None)
    pd.reset_option('display.max_columns')
    return None


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
    print(response)
    census_tract_information = response["result"]["addressMatches"][0]["geographies"]["Census Tracts"][0]
    return census_tract_information["STATE"] + census_tract_information["COUNTY"] + census_tract_information["TRACT"]

# address_1 = Address('1745 T Street Southeast', 'Washington', 'DC', '20020')
# address_2 = Address('6007 Applegate Lane', 'Louisville', 'KY', '40219')
# address_3 = Address('560 Penstock Drive', 'Grass Valley', 'CA', '95945')
# address_4 = Address('150 Carter Street', 'Manchester', 'CT', '06040')
# address_5 = Address('2721 Lindsay Avenue', 'Louisville', 'KY', '20022')
#
# geocode_addresses_to_census_tract([address_1, address_2, address_3, address_4, address_5])
