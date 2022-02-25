import os
import json
import timeit
from address import Address
from geocoder import *

def read_json_file(file):
    with open(file) as address_file:
        address_list = json.load(address_file)['addresses']
    return address_list

def read_json_file_seperate_states(file):
    address_dict = {}
    with open(file) as address_file:
        address_list = json.load(address_file)['addresses']
    for address in address_list:
        state = address['state']
        if state in address_dict.keys():
            address_dict[state].append(address)
        else:
            address_dict.update({state : [address]})
    return address_dict


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


def parse_json_dict_seperate_states(address_dict):
    parsed_dict = {}
    for state in address_dict:
        parsed_addresses = []
        for address in address_dict[state]:
            if 'city' in address.keys():
                street = address['address1']
                city = address['city']
                region = address['state']
                postalcode = address['postalCode']
                parsed_addresses.append(Address(street, city, region, postalcode))
        parsed_dict.update({state : parsed_addresses})
    return parsed_dict


def geocode_list(parsed_addresses, list_range):
    start_time = timeit.default_timer()
    for address in parsed_addresses[:list_range]:
        try:
            geocode_address_to_census_tract(address)
        except(IndexError, KeyError):
            pass
    return (timeit.default_timer() - start_time)/list_range

ad = parse_json_file(read_json_file(os.getcwd()+r'\input\addresses-us-all.json'))

print(geocode_list(ad,100))

