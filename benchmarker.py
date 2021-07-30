import timeit
from itertools import groupby

import sedoh_data_structure as sds
from data_structure import GetStrategy
import value_getter
from functools import reduce
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


benchmarker = Benchmarker()
time = benchmarker.benchmark_acs_api_call_with_single_variable()
print("Time to get one ACS variable via API call: " + str(time))

time, number_of_acs_variables = benchmarker.benchmark_acs_api_call_with_multiple_variables()
print(f"Time to get {number_of_acs_variables} ACS variable via API call: {time}")
