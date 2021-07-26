import value_getter
import progress_bar


class DataFrameEnhancer:
    def __init__(self, data_frame, data_elements):
        self.data_frame = data_frame
        self.data_elements = data_elements

    def add_data_elements(self):
        for data_element in self.data_elements:
            self.data_frame[data_element.variable_name] = ""

    def get_data_element_values(self):
        for index, row in self.data_frame.iterrows():
            progress_bar.progress(index, len(self.data_frame.index), "Enhancing with SEDoH data elements")
            arguments = {"fips_concatenated_code": self.data_frame.iloc[index]['SPATIAL_GEOID']}
            for data_element in self.data_elements:
                self.data_frame.iloc[index][data_element.variable_name] = \
                    value_getter.get_value(data_element, arguments)

    def enhance(self):
        self.add_data_elements()
        self.get_data_element_values()
        return self.data_frame

