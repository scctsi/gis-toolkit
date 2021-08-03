import pandas as pd

class Address:
    def __init__(self, street, city, state, zip):
        self.street = street
        self.city = city
        self.state = state
        self.zip = zip

    @classmethod
    def to_data_frame(cls, addresses):
        list_of_address_dicts = list(map(lambda x: vars(x), addresses))
        return pd.DataFrame(list_of_address_dicts)
