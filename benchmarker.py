import timeit
from itertools import groupby

import geocoder
import sedoh_data_structure as sds
from address import Address
from data_structure import GetStrategy
import value_getter
import re


class Benchmarker:
    def __init__(self):
        # The ACS data elements call the Census API which might potentially affect the enhancement process.
        # This next statement filters out all the ACS data elements
        # and removes data elements that are calculations to benchmark pure Census API calls
        self.acs_data_elements = sds.SedohDataElements().filtered_data_elements(sds.SedohDataSource.ACS,
                                                                                GetStrategy.PRIVATE_API)

    def benchmark_acs_api_call_with_single_variable(self):
        arguments = {"fips_concatenated_code": "06001400100"}

        start_time = timeit.default_timer()
        for index in range(5):
            value_getter.get_acs_value(self.acs_data_elements[0].source_variable, arguments)
        return (timeit.default_timer() - start_time) / 5

    def benchmark_acs_api_call_with_multiple_variables(self):
        arguments = {"fips_concatenated_code": "06001400100"}

        # TODO: Move these functions to a Census module perhaps
        # Step 1: Get a list of all the source variables for the Census API call
        source_variables = list(map(lambda data_element: data_element.source_variable, self.acs_data_elements))

        # Step 2: The Census API call only allows multiple source variables that all belong to a single "table"
        # The ACS source variables can be grouped by their starting letter code
        source_variables_and_code = \
            list(map(lambda data_element: (re.search("^[a-zA-Z]{1,2}", data_element.source_variable).group(),
                                                              data_element.source_variable,
                                                              ), self.acs_data_elements))
        source_variables_and_code = sorted(source_variables_and_code)

        # Step 3: Group the ACS source variables by their letter code
        grouped_acs_data_elements = {}
        for key, group in groupby(source_variables_and_code, lambda x: x[0]):
            grouped_acs_data_elements[key] = [source_variable[1] for source_variable in group]

        # Step 4: For testing, pick the letter code 'S' (which should have the most number of source_variables)
        # for benchmarking. Reduce this to a string concatenating all of the elements of that list.
        source_variables_string = ",".join(grouped_acs_data_elements['S'])

        start_time = timeit.default_timer()
        for index in range(5):
            value_getter.get_acs_values(source_variables_string, arguments)

        return ((timeit.default_timer() - start_time) / 5), len(grouped_acs_data_elements['S'])

    def benchmark_single_geocoder_call(self):
        address = geocoder.Address('1745 T Street Southeast', 'Washington', 'DC', '20020')

        start_time = timeit.default_timer()
        for index in range(5):
            geocoder.geocode_address_to_census_tract(address)
        return (timeit.default_timer() - start_time) / 5

    def benchmark_batch_geocoder_call(self):
        address_1 = Address('1745 T Street Southeast', 'Washington', 'DC', '20020')
        address_2 = Address('6007 Applegate Lane', 'Louisville', 'KY', '40219')
        address_3 = Address('560 Penstock Drive', 'Grass Valley', 'CA', '95945')
        address_4 = Address('150 Carter Street', 'Manchester', 'CT', '06040')
        address_5 = Address('2721 Lindsay Avenue', 'Louisville', 'KY', '20022')

        start_time = timeit.default_timer()
        geocoder.geocode_addresses_to_census_tract([address_1, address_2, address_3, address_4, address_5])
        return timeit.default_timer() - start_time


benchmarker = Benchmarker()

# Benchmark Census Geocoder service
time = benchmarker.benchmark_single_geocoder_call()
print("Time to geocode one physical address via API call: " + str(time))

time = benchmarker.benchmark_batch_geocoder_call()
print("Time to geocode five physical addresses in a single batch via API call: " + str(time))

# Benchmark Census API ACS API calls
time = benchmarker.benchmark_acs_api_call_with_single_variable()
print("Time to get one ACS variable via API call: " + str(time))

time, number_of_acs_variables = benchmarker.benchmark_acs_api_call_with_multiple_variables()
print(f"Time to get {number_of_acs_variables} ACS variable via API call: {time}")

# Calling Census API with multiple variables instead of 1 variable at a time should reduce our runtime by a factor
# of 0.125. NOTE: This is based on our current set of data elements.

