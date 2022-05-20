import pandas as pd

import constant
import geocoder
from data_frame_enhancer import DataFrameEnhancer
from data_structure import DataElement, GetStrategy
import sedoh_data_structure as sds
import value_getter
import importer
import exporter
from optparse import OptionParser

sedoh_data_elements = sds.SedohDataElements()


def load_data_files():
    data_files = {
        sds.SedohDataSource.CalEPA_CES: (importer.import_file("./data_files/calepa_ces/calepa_ces_3.0.xlsx"), "Census Tract"),
        sds.SedohDataSource.CDC: (importer.import_file("./data_files/cdc/cdc_2018.csv"), "FIPS"),
        sds.SedohDataSource.Gazetteer: (importer.import_file("./data_files/gazetteer/gazetteer_2020.txt"), "GEOID"),
        sds.SedohDataSource.USDA: (importer.import_file('./data_files/usda/usda.xls'), "CensusTrac")
    }
    # TODO: This is a fix to add a leading 0 to the CalEPA_CES data file. Get the data from CalEPA to fix this issue.
    calepa_ces_data_file = data_files[sds.SedohDataSource.CalEPA_CES][0]
    calepa_ces_data_file['Census Tract'] = '0' + calepa_ces_data_file['Census Tract']
    return data_files


def get_data_key(file_path):
    index = file_path.rindex('/')
    file_name = file_path[index + 1:]
    index = file_name.index('.')
    return file_name[:index] + '_' + file_name[index + 1:].lower()


def data_key_to_file_name(data_key):
    index = data_key.rindex('_')
    file_name = data_key[:index]
    extension = data_key[index + 1:]
    return file_name, extension


def input_file_validation(data_frame, version, geocode):
    if geocode:
        if not ('street' in data_frame.columns and 'city' in data_frame.columns and
                'state' in data_frame.columns and 'zip' in data_frame.columns):
            raise Exception(f"Input file is missing at least one address column, (street, city, state, zip) are required.")
        if constant.GEO_ID_NAME in data_frame.columns:
            print(f"Warning: You have opted into geocoding, even though your input file already contains a {constant.GEO_ID_NAME} column.")
        city_missing = data_frame.index[data_frame['city'] == ''].tolist()
        zip_missing = data_frame.index[data_frame['zip'] == ''].tolist()
        if len(city_missing) > 0:
            print(f"Warning: {len(city_missing)} rows are missing a city in their address at these indexes: {city_missing}")
        if len(zip_missing) > 0:
            print(f"Warning: {len(zip_missing)} rows are missing a zip code in their address at these indexes: {zip_missing}")
    elif constant.GEO_ID_NAME not in data_frame.columns:
        raise Exception(f"Input file is missing {constant.GEO_ID_NAME} column, and you have not opted into geocoding. "
                        f"Address census tracts are required for enhancement process.")
    elif version == 2 and (constant.LATITUDE not in data_frame.columns or constant.LONGITUDE not in data_frame.columns):
        print(f"Warning: {constant.LATITUDE} and/or {constant.LONGITUDE} columns are missing. Addresses will not be able"
              f"to be enhanced with pollutant data from raster files. Raster file data is geographic and requires the"
              f"latitude and longitude of address to be read.")
    if version == 2:
        address_start_date_missing = data_frame.index[data_frame['address_start_date'] == ''].tolist()
        address_end_date_missing = data_frame.index[data_frame['address_end_date'] == ''].tolist()
        if len(address_start_date_missing) > 0:
            print(f"Warning: {len(address_start_date_missing)} rows are missing an address start date at these indexes: {address_start_date_missing}")
        if len(address_end_date_missing) > 0:
            print(f"Warning: {len(address_end_date_missing)} rows are missing an address end date at these indexes: {address_end_date_missing}")


def main(argument):
    input_file_path = f'./input/{argument.filename}'

    # Step 1: Import the data to be enhanced. Currently supports .csv, .xls, .xlsx
    # Look at supporting Oracle, MySQL, PostgreSQL, SQL Server, REDCap
    data_key = get_data_key(input_file_path)
    file_name, extension = data_key_to_file_name(data_key)
    print(f"Importing input file located at {input_file_path}")
    input_data_frame = importer.import_file(input_file_path, argument.version)
    input_file_validation(input_data_frame, argument.version, argument.geocode)

    data_elements = sds.SedohDataElements().data_elements

    # Setup: Load data files for data sources that do not have an existing API
    print(f"Importing data files")
    if argument.version == 2:
        data_files = sds.DataFiles().data_files
    else:
        data_files = load_data_files()

    # Optional Step: Geocode addresses
    if argument.geocode:
        input_data_frame = geocoder.geocode_addresses_in_data_frame(input_data_frame, data_key,
                                                                    version=argument.version)

    # Step 2: Enhance the data with the requested data elements
    print("Starting enhancement with SEDoH data")
    sedoh_enhancer = DataFrameEnhancer(input_data_frame, data_elements, data_files, data_key, version=argument.version)
    if argument.version == 2:
        sedoh_enhancer.enhance()
    else:
        enhanced_data_frame = sedoh_enhancer.enhance()
        # # Step 3: Export the enhanced data. Currently supports .csv, .xls, .xlsx
        # # Look at supporting Oracle, MySQL, PostgreSQL, SQL Server, REDCap
        exporter.export_file(enhanced_data_frame, f"./output/{file_name}_enhanced.{extension}")
        print(f"Exported enhanced file to ./output/{file_name}_enhanced.{extension}")
    print("Finished enhancement with SEDoH data")


if __name__ == "__main__""":
    parser = OptionParser()
    parser.add_option('-f', '--file', type='string', dest='filename', default='addresses.xlsx',
                      help='name of input file: string')
    parser.add_option('-v', '--version', type='int', dest='version', default=1, help='version of gis-toolkit: int')
    parser.add_option('-g', '--geocode', type='choice', dest='geocode', default=None, choices=['geocode'],
                      help='turn on geocoding, default is off')
    (options, args) = parser.parse_args()
    main(options)
