import os
import json
import timeit
from address import Address
from geocoder import *

def read_json_file(file):
    with open(file) as address_file:
        address_list = json.load(address_file)['addresses']
    return address_list


def parse_json_file(address_list):
    parsed_addresses = []
    for address in address_list:
        if 'city' in address.keys():
            street = address['address1']
            city = address['city']
            region = address['state']
            postalcode = address['postalCode']
            parsed_addresses.append(Address(street, city, region, postalcode))
    return parsed_addresses


def geocode_address_list(parsed_addresses, list_range):
    start_time = timeit.default_timer()
    for address in parsed_addresses[:list_range]:
        try:
            geocode_address_to_census_tract(address)
        except(IndexError, KeyError):
            pass
    return (timeit.default_timer() - start_time)/list_range


def geocode_addresses_list(parsed_addresses, list_range):
    start_time = timeit.default_timer()
    geocode_addresses_to_census_tract(parsed_addresses[:list_range], "rrad test addresses")
    return (timeit.default_timer() - start_time) / list_range


rrad_addresses = parse_json_file(read_json_file('./validation/addresses-us-all.json'))
address_count = 5
geocoding_avg_time = round(geocode_addresses_list(rrad_addresses, address_count), 3)
print(str(geocoding_avg_time) + " seconds per address to batch geocode " + str(address_count) + " addresses")
