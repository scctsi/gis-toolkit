import json
import os.path
import requests
import importer
from config import enhancement_config
import sedoh_data_structure as sds
import pandas as pd
import pathlib


def check_data_sources():
    from sedoh_data_structure import SedohDataSource
    with open('./data_files_key.json') as save_file:
        data = json.load(save_file)
    data_elements = sds.SedohDataElements().data_elements
    for data_source, source_dict in data.items():
        sedoh_data_source = vars(SedohDataSource)["_member_map_"][data_source]
        if sedoh_data_source in [SedohDataSource.SCEHSC, SedohDataSource.NASA]:
            for pollutant, pollutant_dict in source_dict.items():
                data_source_data_elements = [data_element for data_element in data_elements if data_element.data_source == (sedoh_data_source, pollutant)]
                if True in [enhancement_config[data_element.variable_name] for data_element in data_source_data_elements]:
                    check_data_files(pollutant_dict['versions'])
        elif sedoh_data_source == SedohDataSource.Gazetteer:
            if enhancement_config['population_density']:
                check_data_files(source_dict['versions'])
        elif sedoh_data_source == SedohDataSource.ACS:
            continue
        else:
            data_source_data_elements = [data_element for data_element in data_elements if data_element.data_source == sedoh_data_source]
            for data_element in data_source_data_elements:
                print(data_element.variable_name)
            if True in [enhancement_config[data_element.variable_name] for data_element in data_source_data_elements]:
                check_data_files(source_dict['versions'])


def check_data_files(versions):
    data_files_dir = './data_files_2'
    for data_file in versions:
        if not os.path.exists(f"{data_files_dir}/{data_file['file_path']}"):
            download_and_write_data_file(data_file, data_files_dir)


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
    resp = session.get(data_file['url'], verify=False)
    if resp.headers["Content-Type"] in ["text/csv", "application/octet-stream"]:
        df = pd.read_csv(data_file['url'])
        df.to_csv(f"{data_files_dir}/{data_file['file_path']}")
    elif resp.headers["Content-Type"] in ["application/zip", "application/x-zip-compressed"]:
        index_folder = data_file['file_path'].rindex('/')
        folder_dir = data_file['file_path'][:index_folder]
        path = f"{data_files_dir}/{folder_dir}"
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
        data_frame = importer.import_file(f"{path}/{data_file['file_name']}")
        data_frame.to_csv(f"{data_files_dir}/{data_file['file_path']}")
    elif data_file['file_name'].endswith('.csv'):
        os.rename(f"{path}/{data_file['file_name']}", f"{data_files_dir}/{data_file['file_path']}")
    elif data_file['file_name'].endswith('.txt'):
        data_frame = pd.read_csv(f"{path}/{data_file['file_name']}")
        data_frame.to_csv(f"{data_files_dir}/{data_file['file_path']}")
