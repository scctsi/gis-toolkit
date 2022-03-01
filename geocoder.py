# Test data: https://github.com/EthanRBrown/rrad which is taken from https://openaddresses.io/
from ssl import VerifyFlags
import requests

import api
from dotenv import load_dotenv
import os
import sys
from io import StringIO
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
    addresses_data_frame.to_csv('./temp/addresses.csv', header=False, index=True)

    api_url = "https://geocoding.geo.census.gov/geocoder/geographies/addressbatch"
    payload = {'benchmark': BENCHMARK, 'vintage': VINTAGE}
    files = {'addressFile': ('addresses.csv', open('./temp/addresses.csv', 'rb'), 'text/csv')}
    response = requests.post(api_url, files=files, data=payload)

    column_names = ["address_id", "input_address", "match_indicator", "match_type", "output_address",
                    "latitude_longitude", "line_id", "line_id_side",
                    "state_code", "county_code", "tract_code", "block_code"]
    geocoded_addresses_data_frame = pd.read_csv(StringIO(response.text), sep=",", names=column_names, dtype='str')
    geocoded_addresses_data_frame['census_tract'] = geocoded_addresses_data_frame['state_code'] + geocoded_addresses_data_frame['county_code'] + geocoded_addresses_data_frame['tract_code']
    geocoded_addresses_data_frame.to_csv('./output/geocoded_addresses.csv')
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
