import requests
import constant


# API specific methods
def construct_url(interpolation_string, arguments):
    return interpolation_string.format(**arguments)


def get_response(url):
    # TODO: Assert 200
    response = requests.get(url)

    try:
        return response.json()
    except Exception:
        if response.status_code == 204:
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


def get_values(url):
    response = get_response(url)
    if response == constant.NOT_AVAILABLE:
        return constant.NOT_AVAILABLE
    else:
        return response_to_dict(response)
