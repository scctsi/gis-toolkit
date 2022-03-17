# Test data: https://github.com/EthanRBrown/rrad which is taken from https://openaddresses.io/
import requests

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


def geocode_addresses_in_data_frame(data_frame, data_key):
    """
    :param data_key: Key of save file, associated with a file's geocoding process
    :param data_frame: Data frame of addresses, to be geocoded
    :return: Data frame with new "SPATIAL_GEOID" column, to be enhanced
    """
    # TODO: Create addresses list using DataFrame function
    data_frame[constant.GEO_ID_NAME] = ''
    addresses = []
    for index, row in data_frame.iterrows():
        addresses.append(Address(row['street'], row['city'], row['state'], row['zip']))
    try:
        geocode_addresses_to_census_tract(addresses, data_key)
        data_frame[constant.GEO_ID_NAME] = importer.import_file('./temp/geocoded_' + data_key + '.csv')['census_tract']
    except Exception as e:
        raise Exception(e)
    return data_frame


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
            data[data_key] = {'last_successful_batch' : batch_index, "status" : "Incomplete", "error_message" : ""}
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
        if data_key in data.keys() and data[data_key]['status'] == "Incomplete":
            batch_index = data[data_key]['last_successful_batch']
        else:
            batch_index = 0
            save_geocode_progress(data_key, batch_index)
    return batch_index


def geocode_addresses_to_census_tract(addresses, data_key, batch_limit=10000):
    """
    Batch geocodes more than 10,000 addresses

    :param addresses: List of addresses of type Address
    :param data_key: Key of save file, associated with a file's geocoding process
    :param batch_limit: Default is census geocoder's maximum batch size
    :return: None
    """
    check_temp_dir()
    batch_calls = int(len(addresses) / batch_limit)
    # If there is a remainder, another batch is added to accomodate those addresses
    if len(addresses) % batch_limit != 0:
        batch_calls += 1
    api_url = "https://geocoding.geo.census.gov/geocoder/geographies/addressbatch"
    payload = {'benchmark': BENCHMARK, 'vintage': VINTAGE}
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
            response = requests.post(api_url, files=files, data=payload, verify=False)
        except requests.exceptions.RequestException as e:
             save_geocode_progress(data_key, i, "Incomplete", str(e))
             raise SystemExit(e)
        # Geocoded address can be returned in a different order, the following lines correct their indicies and sort them
        geocoded_address_batch_data_frame = pd.read_csv(StringIO(response.text), sep=",", names=column_names, dtype='str')
        geocoded_address_batch_data_frame.index = geocoded_address_batch_data_frame['address_id'].astype(int).add(i * batch_limit)
        geocoded_address_batch_data_frame['address_id'] = geocoded_address_batch_data_frame.index
        geocoded_address_batch_data_frame = geocoded_address_batch_data_frame.sort_index()
        # The values created in 'census tract' are the "SPATIAL_GEOID" used in the enhancement process
        geocoded_address_batch_data_frame['census_tract'] = geocoded_address_batch_data_frame['state_code'] + \
                                                        geocoded_address_batch_data_frame['county_code'] + \
                                                        geocoded_address_batch_data_frame['tract_code']
        if i == 0:
            geocoded_address_batch_data_frame.to_csv('./temp/geocoded_' + data_key + '.csv')
        else:
            geocoded_address_batch_data_frame.to_csv('./temp/geocoded_' + data_key + '.csv', header=False, mode='a')
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
