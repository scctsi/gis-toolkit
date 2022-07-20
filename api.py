import pandas as pd
import requests
import constant


# API specific methods
def construct_url(interpolation_string, arguments):
    return interpolation_string.format(**arguments)


def get_response(url, test_mode=False):
    # TODO: Assert 200
    if not test_mode:
        response = requests.get(url)
    else:
        response = requests.get(url, verify=False)
    try:
        return response.json()
    except Exception:
        if response.status_code == 400:
            return constant.NOT_AVAILABLE
        else:
            print(response)
            quit(1)


def get_batch_values(url, test_mode=False):
    response = get_response(url, test_mode)
    if response == constant.NOT_AVAILABLE:
        return constant.NOT_AVAILABLE
    else:
        data_frame = pd.DataFrame(data=response[1:], columns=response[0], dtype="str")
        data_frame = data_frame.loc[:,~data_frame.columns.duplicated()]
        data_frame[constant.GEO_ID_NAME] = data_frame['state'] + data_frame['county'] + data_frame['tract']
        data_frame.index = data_frame[constant.GEO_ID_NAME]
        return data_frame
