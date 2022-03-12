import csv
import timeit

import pandas as pd

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


def json_to_data_frame(address_list):
    parsed_addresses = []
    for address in address_list:
        if 'city' in address.keys():
            street = address['address1']
            city = address['city']
            region = address['state']
            postalcode = address['postalCode']
            parsed_addresses.append([street, city, region, postalcode])
    with open('./validation/addresses-us-all.csv', 'w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(['street', 'city', 'state', 'zip'])
        writer.writerows(parsed_addresses)
    json_data_frame = pd.read_csv('./validation/addresses-us-all.csv', dtype='str')
    return json_data_frame

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

def parse_data_frame3(data_frame):
    start_time = timeit.default_timer()
    addresses = []
    for row in data_frame.itertuples():
        addresses.append(Address(row.street, row.city, row.state, row.zip))
    return timeit.default_timer() - start_time

def parse_data_frame1(data_frame):
    start_time = timeit.default_timer()
    addresses = []
    for index, row in data_frame.iterrows():
        addresses.append(Address(row['street'], row['city'], row['state'], row['zip']))
    return timeit.default_timer() - start_time


def parse_data_frame2(data_frame):
    start_time = timeit.default_timer()
    addresses = []
    data_frame.apply(lambda row: addresses.append(Address(row.street, row.city, row.state, row.zip)), axis=1)
    return timeit.default_timer() - start_time


# rrad_addresses = parse_json_file(read_json_file('./validation/addresses-us-all.json'))
# address_count = 5
# geocoding_avg_time = round(geocode_addresses_list(rrad_addresses, address_count), 3)
# print(str(geocoding_avg_time) + " seconds per address to batch geocode " + str(address_count) + " addresses")

rrad_addresses_data_frame = pd.read_csv('./validation/addresses-us-all.csv', dtype='str')
print(parse_data_frame1(rrad_addresses_data_frame))
print(parse_data_frame2(rrad_addresses_data_frame))
print(parse_data_frame3(rrad_addresses_data_frame))