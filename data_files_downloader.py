import json
import os.path
import requests
import sedoh_data_structure as sds
import pandas as pd


def check_data_sources(directory, enhancement):
    from sedoh_data_structure import SedohDataSource
    with open('./data_files_key.json') as save_file:
        data = json.load(save_file)
    data_elements = sds.SedohDataElements().data_elements
    for data_source, source_dict in data.items():
        sedoh_data_source = vars(SedohDataSource)["_member_map_"][data_source]
        if sedoh_data_source in [SedohDataSource.SCEHSC, SedohDataSource.NASA]:
            for pollutant, pollutant_dict in source_dict.items():
                data_source_data_elements = [data_element for data_element in data_elements if data_element.data_source == (sedoh_data_source, pollutant)]
                if True in [enhancement[data_element.variable_name] for data_element in data_source_data_elements]:
                    check_data_files(pollutant_dict['versions'], directory)
        elif sedoh_data_source == SedohDataSource.Gazetteer:
            if enhancement['population_density']:
                check_data_files(source_dict['versions'], directory)
        elif sedoh_data_source == SedohDataSource.ACS:
            continue
        else:
            data_source_data_elements = [data_element for data_element in data_elements if data_element.data_source == sedoh_data_source]
            if True in [enhancement[data_element.variable_name] for data_element in data_source_data_elements]:
                check_data_files(source_dict['versions'], directory)


def check_data_files(versions, directory):
    for data_file in versions:
        if not os.path.exists(f"./{directory}/{data_file['file_path']}"):
            print(data_file['file_path'])
            download_and_write_data_file(data_file, directory)


class SessionWithHeaderRedirection(requests.Session):
    AUTH_HOST = 'urs.earthdata.nasa.gov'

    def __init__(self, username, password):

        super().__init__()

        self.auth = (username, password)

    def rebuild_auth(self, prepared_request, response):
        import requests.utils
        headers = prepared_request.headers
        url = prepared_request.url

        if 'Authorization' in headers:
            original_parsed = requests.utils.urlparse(response.request.url)
            redirect_parsed = requests.utils.urlparse(url)

            if (original_parsed.hostname != redirect_parsed.hostname) and \
                    redirect_parsed.hostname != self.AUTH_HOST and \
                    original_parsed.hostname != self.AUTH_HOST:

                del headers['Authorization']
        return


def download_and_write_data_file(data_file, data_files_dir):
    import zipfile, io
    print(data_file)
    session = SessionWithHeaderRedirection(os.getenv('earth_data_username'), os.getenv('earth_data_password'))
    try:
        resp = session.get(data_file['url'], verify=False)
    except:
        raise Exception("Earth Data Error")

    index_folder = data_file['file_path'].rindex('/')
    folder_dir = data_file['file_path'][:index_folder]
    path = f"./{data_files_dir}/{folder_dir}"

    if not os.path.exists(f'./{data_files_dir}'):
        os.mkdir(f'./{data_files_dir}')

    if not os.path.exists(path):
        if data_file['file_path'].startswith('nasa/') and not os.path.exists(f"./{data_files_dir}/nasa"):
            os.mkdir(f"./{data_files_dir}/nasa")
        os.mkdir(path)

    if resp.headers["Content-Type"] in ["text/csv", "application/octet-stream"]:
        df = pd.read_csv(data_file['url'], dtype=str)
        correct_data_frame(df, data_file)
        df.to_csv(f"./{data_files_dir}/{data_file['file_path']}")

    elif resp.headers["Content-Type"] in ["application/zip", "application/x-zip-compressed"]:
        z = zipfile.ZipFile(io.BytesIO(resp.content))
        z.extractall(path=path)
        if data_file['file_path'].startswith('nasa/'):
            rename_nasa_files(path)
        else:
            handle_extracted_file(data_file, path, data_files_dir)


def rename_nasa_files(path):
    if 'PM25' in path:
        for file in os.listdir(f"{path}/Annual-geotiff"):
            os.rename(f"{path}/Annual-geotiff/{file}", f"{path}/{file}")
    elif 'O3' in path:
        for file in os.listdir(path):
            index_last = file.rindex('_') + 1
            os.rename(f"{path}/{file}", f"{path}/{file[index_last:]}")


def handle_extracted_file(data_file, path, data_files_dir):
    if data_file['file_name'].endswith('.xlsx'):
        data_frame = pd.read_excel(f"{path}/{data_file['file_name']}", sheet_name=data_file['sheet_name'], dtype=str)
    elif data_file['file_name'].endswith('.csv'):
        data_frame = pd.read_csv(f"{path}/{data_file['file_name']}", dtype=str)
    elif data_file['file_name'].endswith('.txt'):
        data_frame = pd.read_csv(f"{path}/{data_file['file_name']}", sep='\t', dtype=str)
    data_frame = correct_data_frame(data_frame, data_file)
    data_frame.to_csv(f"./{data_files_dir}/{data_file['file_path']}")


def correct_data_frame(data_frame, data_file):
    data_frame = correct_census_tracts(data_frame, data_file)
    if data_file['file_path'] == 'cdc/cdc_2000.csv':
        data_frame.rename(columns={'USTP': 'RPL_THEMES'}, inplace=True)
    elif data_file['file_path'] == 'cdc/cdc_2010.csv':
        data_frame.rename(columns={'R_PL_THEMES': 'RPL_THEMES'}, inplace=True)
    elif data_file['file_path'] in ["usda/usda_2010.csv",  "usda/usda_2015.csv"]:
        data_frame = correct_usda_units(data_frame)
    return data_frame


def correct_census_tracts(data_frame, data_file):
    data_frame[data_file["census_tract_column_name"]] = data_frame.apply(
        lambda col: correct_census_tract(col[data_file["census_tract_column_name"]]), axis=1)
    return data_frame


def correct_census_tract(census_tract):
    if len(census_tract) == 10:
        return '0' + census_tract
    else:
        return census_tract


def correct_usda_units(data_frame):
    data_frame['lapop1share'] = convert_series_from_proportion_to_percentage(data_frame['lapop1share'])
    data_frame['lapop10share'] = convert_series_from_proportion_to_percentage(data_frame['lapop10share'])
    return data_frame


def convert_series_from_proportion_to_percentage(series):
    series = pd.to_numeric(series)
    series = series * 100
    series = series.astype(str)
    return series

