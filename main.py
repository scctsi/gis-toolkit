from data_frame_enhancer import DataFrameEnhancer
from data_structure import DataElement, GetStrategy
import sedoh_data_structure as sds
import value_getter
import importer
import exporter


def load_data_files():
    data_files = {
        sds.SedohDataSource.CalEPA_CES: (importer.import_file("./data_files/calepa_ces.xlsx"), "Census Tract"),
        sds.SedohDataSource.CDC: (importer.import_file("./data_files/cdc.csv"), "FIPS"),
        sds.SedohDataSource.Gazetteer: (importer.import_file("./data_files/gazetteer.txt"), "GEOID"),
        sds.SedohDataSource.USDA: (importer.import_file('./data_files/usda.xls'), "CensusTrac")
    }

    print(data_files[sds.SedohDataSource.sds.SedohDataSource.CalEPA_CES][0].head())

    return data_files


def main():
    sedoh_data_elements = sds.SedohDataElements()
    # Setup: Load data files for data sources that do not have an existing API
    print(f"Importing data files")
    data_files = load_data_files()

    # Step 1: Import the data to be enhanced. Currently supports .csv, .xls, .xlsx
    # Look at supporting Oracle, MySQL, PostgreSQL, SQL Server, REDCap
    test_file_path = './input/single_geocoded.xlsx'
    print(f"Importing input file located at {test_file_path}")
    input_data_frame = importer.import_file(test_file_path)

    # Step 2: Enhance the data with the requested data elements
    print("Starting enhancement with SEDoH data")
    sedoh_enhancer = DataFrameEnhancer(input_data_frame, sedoh_data_elements.data_elements, data_files)
    enhanced_data_frame = sedoh_enhancer.enhance()
    print("Finished enhancement with SEDoH data")

    # # Step 3: Export the enhanced data. Currently supports .csv, .xls, .xlsx
    # # Look at supporting Oracle, MySQL, PostgreSQL, SQL Server, REDCap
    exporter.export_file(enhanced_data_frame, "./output/geocoded.xlsx")
    print("Exported file located at ./output/geocoded.xlsx")

    # test_data_element = DataElement(sedoh_data_structure.SedohDataSource.ACS, "Gini Inequality Coefficient",
    #                                 "gini_inequality_coefficient", "B19083_001E", GetStrategy.PUBLIC_API)
    #
    # value_getter.get_value(test_data_element, {"state_code": "06", "county_code": "001", "tract_code": "400100"})


if __name__ == "__main__""":
    main()
