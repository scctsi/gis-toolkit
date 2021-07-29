from enum import Enum


class GetStrategy(Enum):
    PUBLIC_API = 1
    PRIVATE_API = 2
    CALCULATION = 3
    FILE = 4
    FILE_AND_CALCULATION = 5


class DataElement:
    def __init__(self, data_source, friendly_name, variable_name, source_variable, get_strategy):
        self.data_source = data_source         # External data source
        self.friendly_name = friendly_name     # A friendly name for the data element
        self.variable_name = variable_name     # A variable name that can be used by most statistical software
        self.source_variable = source_variable # The name of the variable at the data source
        self.get_strategy = get_strategy       # How do we acquire the value of this variable?


# Currently not used
# class DataStructure:
#     def __init__(self, data_elements):
#         self.data_elements = data_elements
