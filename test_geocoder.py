import os
import json
import pandas as pd
import csv
from address import Address
from geocoder import *


"""
This script collects fake addresses of multiple states downloaded from openaddresses.io. Currently it reads and parses 100 previously 
filtered addresses of each state from the test_addresses folder using read_test_addresses(). Then it passes those fake addresses into 
the geocoder to identify how well the geocoder can geocode each state using geocode_dict().

Fake addresses were filtered using the following method:
The openaddresses.io download folder containing fake addresses of each state is opened using read_json_files(), each file in each state folder
is checked to see if it contains sufficient address data using check_address_validity(). Each state is then ascribed a list of "good" address files
from different the counties and cities of that state. Then addresses are read and parsed from these files such that roughly 100 addresses, of as much
geographic variety as our data allows, are ascribed to each state. (names of state folders from openaddresses.io may need to capitalized)

Outputs of read_test_addresses() and read_json_files() must be passed through parse_json_dict() before they can be passed through geocode_dict()
"""

def capitalize_states(folder_path):
    for folder in os.listdir(folder_path):
        os.rename(folder, folder.upper())
    return None


def check_address_validity(data, state):
    """
    parameters:
        data, json formatted address: str
        state, state code ex. "CA": str

    rtype: bool
    """
    if(data['properties']['number'] != "" and data['properties']['street'] != "" and data['properties']['city'] != "" and data['properties']['region'] != "" and data['properties']['postcode'] != "" and data['properties']['number'] != " " and data['properties']['street'] != " " and data['properties']['city'] != " " and data['properties']['region'] != " " and data['properties']['postcode'] != " " and len(data['properties']['city']) < 40 and data['properties']['region']==state):
        return True
    else:
        return False


def read_json_file(file_name, file_range, state):
    """
    parameters:
        file_name, directory of file to open: str
        file_range, # of lines to read from file: int
        state, state code ex. "CA": str
    
    rtype: [dict]
    """
    addresses = []
    count = 0
    with open(file_name) as address_file:
        for line in address_file:
            if(count==file_range):
                break
            address = json.loads(line)
            if(check_address_validity(address, state)):
                addresses.append(address)
                count+=1
    address_file.close()
    return addresses


def good_file(file_path, state):
    """
    parameters:
        file_path, directory of folder including address files: str
        state, state code ex. "CA": str
    
    returns: list of directories of well formatted files from file_path folder: [str]
    """
    file_names = []
    for file in os.listdir(file_path):
        file_name = f'{file_path}/{file}'
        if(file.endswith('addresses-county.geojson') or file.endswith('addresses-city.geojson')):
            with open(file_name) as address_file:
                for line in address_file:
                    address = json.loads(line)
                    break
            address_file.close()
            if(check_address_validity(address, state)):
                file_names.append(file_name)
    return file_names


def read_json_files(file_path, file_range):
    """
    parameters:
        file_path, directory of folder containing folders of address data of each state: str
        file_range, number of addresses to read per state: int

    returns: {state code : list of addresses}
    """
    state_addresses = {}
    for folder in os.listdir(file_path):
        state_name = folder
        state_list = []
        temp_cwd = f'{file_path}/{folder}'
        file_names = good_file(temp_cwd, state_name)
        if(len(file_names)!=0):
            state_range = (int)(file_range/len(file_names))
            for file_name in file_names:
                state_list = state_list + read_json_file(file_name, state_range, state_name)
        state_addresses.update({state_name : state_list})
    return state_addresses


def parse_json_dict(address_dict):
    """
    parameters:
        address_dict, accepts output of read_json_files() and read_test_addresses()
    
    returns: {state code : list of address class}
    """
    parsed_dict = {}
    for state in address_dict:
        parsed_addresses = []
        for address in address_dict[state]:
            street = address['properties']['number']+" "+address['properties']['street']
            city = address['properties']['city']
            region = address['properties']['region']
            postcode = address['properties']['postcode']
            parsed_addresses.append(Address(street, city, region, postcode))
        parsed_dict.update({state : parsed_addresses})
    return parsed_dict


def geocode_dict(address_dict):
    """
    parameter:
        address_dict, accepts output of parse_address_dict()
    
    returns: [[state code : geocodability %]]
    """
    results_list = []
    for state in address_dict:
        print("OPEN STATE: " + state)
        den = len(address_dict[state])
        num = 0
        for i, address in enumerate(address_dict[state]):
            print("ADDRESS: " + str(i))
            try:
                geocode_address_to_census_tract(address)
                num += 1
            except (IndexError, KeyError):
                pass
        if(den > 0):
            geocodability = round(100*(num/den), 2)
        else:
            geocodability = -1
        print("CLOSE STATE: "+state, "GEOCODABILITY: " + str(geocodability))
        results_list.append([state, geocodability])
    return results_list


def read_test_addresses_from_csv():
    parsed_dict = {}
    folder_path = './validation/test_addresses/'
    for file in os.listdir(folder_path):
        state_name = file.replace('.csv', '')
        state_list = []
        state_addresses_data_frame = pd.read_csv(f'{folder_path}/{file}', dtype='str')
        for index, row in state_addresses_data_frame.iterrows():
            state_list.append(Address(row['street'], row['city'], row['state'], row['zip']))
        parsed_dict.update({state_name: state_list})
    return parsed_dict


address_data = read_test_addresses_from_csv()
# open_addresses_file_path = "<Add_OpenAddresses_Folder_Directory_Here>"
# capitalize_states(open_addresses_file_path)
# address_data = parse_json_dict(read_json_files(open_addresses_file_path, 100))


# result_data = geocode_dict(address_data)
# with open('./validation/geocoded_percentage.csv', 'w', newline='') as result_file:
#     writer = csv.writer(result_file)
#     writer.writerow(['state', 'geocoded_percentage'])
#     writer.writerows(result_data)
