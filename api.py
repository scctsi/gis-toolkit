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


def get_header_row_and_truncated_json(json_to_process):
    header_row = json_to_process[0].copy()
    del json_to_process[0]

    return header_row, json_to_process


def response_to_dict(response):
    result_dict = {}
    for i, source in enumerate(response[0]):
        if source == 'state':
            break
        result_dict.update({source: response[1][i]})
    return result_dict


def get_value(url):
    response = get_response(url)
    if response == constant.NOT_AVAILABLE:
        return constant.NOT_AVAILABLE
    else:
        print(response)
        header_row, truncated_response = get_header_row_and_truncated_json(response)
        # TODO: This is specific to the Census API which returns JSON like this example below:
        # [['B19083_001E', 'state', 'county', 'tract'], ['0.4981', '06', '001', '400100']]
        # For now, only ACS has an API, but this function needs expansion once we have more API data sources
        return truncated_response[0][0]


def get_values(url, test_mode=False):
    response = get_response(url, test_mode)
    if response == constant.NOT_AVAILABLE:
        return constant.NOT_AVAILABLE
    else:
        print(response)
        return response_to_dict(response)


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
