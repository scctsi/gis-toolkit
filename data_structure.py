from enum import Enum


class GetStrategy(Enum):
    PUBLIC_API = 1
    PRIVATE_API = 2
    CALCULATION = 3
    FILE = 4


class DataElement:
    def __init__(self, source, friendly_name, variable_name, source_variable, get_strategy):
        self.source = source
        self.friendly_name = friendly_name
        self.variable_name = variable_name
        self.source_variable = source_variable
        self.get_strategy = get_strategy


# Currently not used
# class DataStructure:
#     def __init__(self, data_elements):
#         self.data_elements = data_elements
