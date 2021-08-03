# Test data: https://github.com/EthanRBrown/rrad which is taken from https://openaddresses.io/
import requests

import api
from dotenv import load_dotenv
import os
import pandas as pd

import constant
from address import Address

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


def geocode_addresses_to_census_tract(addresses):
    addresses_data_frame = Address.to_data_frame(addresses)
    addresses_data_frame.to_csv('./temp/addresses.csv')

    api_url = "https://geocoding.geo.census.gov/geocoder/geographies/addressbatch"
    payload = {'benchmark': BENCHMARK, 'vintage': VINTAGE}
    files = {'addressFile': ('addresses.csv', open('./temp/addresses.csv', 'rb'), 'text/csv')}
    r = requests.post(api_url, files=files, data=payload)
    print(r.text)
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