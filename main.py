from data_frame_enhancer import DataFrameEnhancer
from data_structure import DataElement, GetStrategy
import sedoh_data_structure
import value_getter
import importer
import exporter

# Step 1: Import the data to be enhanced. Currently supports .csv, .xls, .xlsx
# Look at supporting Oracle, MySQL, PostgreSQL, SQL Server, REDCap
print("Importing input file located at ./input/geocoded.xlsx")
input_data_frame = importer.import_file('./input/geocoded.xlsx')
input_data_frame.info()
print(len(input_data_frame.index))

# Step 2: Enhance the data with the requested data elements
print("Starting enhancement with SEDoH data")
sedoh_enhancer = DataFrameEnhancer(input_data_frame, sedoh_data_structure.sedoh_data_elements)
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
