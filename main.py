import geocoder
from data_frame_enhancer import DataFrameEnhancer
from data_structure import DataElement, GetStrategy
import sedoh_data_structure as sds
import value_getter
import importer
import exporter

sedoh_data_elements = sds.SedohDataElements()


def load_data_files():
    data_files = {
        sds.SedohDataSource.CalEPA_CES: (importer.import_file("./data_files/calepa_ces.xlsx"), "Census Tract"),
        sds.SedohDataSource.CDC: (importer.import_file("./data_files/cdc.csv"), "FIPS"),
        sds.SedohDataSource.Gazetteer: (importer.import_file("./data_files/gazetteer.txt"), "GEOID"),
        sds.SedohDataSource.USDA: (importer.import_file('./data_files/usda.xls'), "CensusTrac")
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


def main():
    data_elements = sds.SedohDataElements().data_elements

    # elements = sds.SedohDataElements().data_elements
    # Setup: Load data files for data sources that do not have an existing API
    print(f"Importing data files")
    data_files = load_data_files()

    # Step 1: Import the data to be enhanced. Currently supports .csv, .xls, .xlsx
    # Look at supporting Oracle, MySQL, PostgreSQL, SQL Server, REDCap
    test_file_path = './input/addresses.xlsx'
    data_key = get_data_key(test_file_path)
    print(f"Importing input file located at {test_file_path}")
    input_data_frame = importer.import_file(test_file_path)

    # Optional Step: Geocode addresses
    if geocoder.geocodable(input_data_frame):
        input_data_frame = geocoder.geocode_data_frame(input_data_frame)

    # Step 2: Enhance the data with the requested data elements
    print("Starting enhancement with SEDoH data")
    sedoh_enhancer = DataFrameEnhancer(input_data_frame, data_elements, data_files)
    enhanced_data_frame = sedoh_enhancer.enhance()
    print("Finished enhancement with SEDoH data")

    # # Step 3: Export the enhanced data. Currently supports .csv, .xls, .xlsx
    # # Look at supporting Oracle, MySQL, PostgreSQL, SQL Server, REDCap
    exporter.export_file(enhanced_data_frame, "./output/enhanced.xlsx")
    print("Exported enhanced file to ./output/enhanced.xlsx")

    # test_data_element = DataElement(sedoh_data_structure.SedohDataSource.ACS, "Gini Inequality Coefficient",
    #                                 "gini_inequality_coefficient", "B19083_001E", GetStrategy.PUBLIC_API)
    #
    # value_getter.get_value(test_data_element, {"state_code": "06", "county_code": "001", "tract_code": "400100"})


if __name__ == "__main__""":
    main()
