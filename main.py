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


def input_file_validation(data_frame, version):
    if version in [None, 1, 2]:
        if constant.GEO_ID_NAME not in data_frame.columns and not (
                'street' in data_frame.columns and
                'city' in data_frame.columns and
                'state' in data_frame.columns and
                'zip' in data_frame.columns):
            raise Exception(f"Input file has missing address columns or a missing {constant.GEO_ID_NAME} column.")
        if constant.GEO_ID_NAME not in data_frame.columns:
            city_missing = data_frame.index[data_frame['city'] == ''].tolist()
            zip_missing = data_frame.index[data_frame['zip'] == ''].tolist()
            if len(city_missing) > 0:
                print(f"{len(city_missing)} rows are missing a city in their address at these indexes: {city_missing}")
            if len(zip_missing) > 0:
                print(f"{len(zip_missing)} rows are missing a zip code in their address at these indexes: {zip_missing}")
        if version == 2:
            address_start_date_missing = data_frame.index[data_frame['address_start_date'] == ''].tolist()
            address_end_date_missing = data_frame.index[data_frame['address_end_date'] == ''].tolist()
            if len(address_start_date_missing) > 0:
                print(f"{len(address_start_date_missing)} rows are missing an address start date at these indexes: {address_start_date_missing}")
            if len(address_end_date_missing) > 0:
                print(f"{len(address_end_date_missing)} rows are missing an address end date at these indexes: {address_end_date_missing}")
    else:
        raise Exception("Invalid version number. Version can be 1 or 2 (default is 1).")


def main(options):
    if options.filename:
        input_file_path = f'./input/{options.filename}'
    else:
        input_file_path = './input/addresses.xlsx'

    # Step 1: Import the data to be enhanced. Currently supports .csv, .xls, .xlsx
    # Look at supporting Oracle, MySQL, PostgreSQL, SQL Server, REDCap
    data_key = get_data_key(input_file_path)
    file_name, extension = data_key_to_file_name(data_key)
    print(f"Importing input file located at {input_file_path}")
    input_data_frame = importer.import_file(input_file_path, options.version)
    input_file_validation(input_data_frame, options.version)

    data_elements = sds.SedohDataElements().data_elements

    # Setup: Load data files for data sources that do not have an existing API
    print(f"Importing data files")
    if options.version == 2:
        data_files = sds.DataFiles().data_files
    else:
        data_files = load_data_files()

    # Optional Step: Geocode addresses
    if geocoder.geocodable(input_data_frame):
        input_data_frame = geocoder.geocode_addresses_in_data_frame(input_data_frame, data_key, version=options.version)

    # Step 2: Enhance the data with the requested data elements
    print("Starting enhancement with SEDoH data")
    sedoh_enhancer = DataFrameEnhancer(input_data_frame, data_elements, data_files, data_key, version=options.version)
    if options.version == 2:
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
    parser.add_option('-f', '--file', type='string', dest='filename', help='name of input file: string')
    parser.add_option('-v', '--version', type='int', dest='version', help='version of gis-toolkit: int')
    (options, args) = parser.parse_args()
    main(options)
