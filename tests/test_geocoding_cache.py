import importer
import geocoder
import pytest
from config import database_config


@pytest.fixture(autouse=True)
def run_around_tests():
    yield
    # if os.path.exists('./temp'):
    #     shutil.rmtree('./temp', ignore_errors=False, onerror=handle_remove_readonly)
    import sqlite3
    conn = sqlite3.connect(database_config)
    c = conn.cursor()
    c.execute(f'''
                  DROP TABLE IF EXISTS geo_cache
                  ''')
    conn.commit()


# test latest then comp
# test comp then latest
# tests with small batch limit


def test_add_line():
    file_path_1 = './tests/geocoding_cache_input_1.csv'
    file_path_2 = './tests/geocoding_cache_input_2.csv'
    output_path = './tests/geocoding_cache_output.csv'
    version = "latest"
    input_data_frame_1 = importer.import_file(file_path_1, version=version)
    input_data_frame_2 = importer.import_file(file_path_2, version=version)

    geocoder.geocode_data_frame(input_data_frame_1.copy(), version=version)
    geocoded_data_frame = geocoder.geocode_data_frame(input_data_frame_2.copy(), version=version)

    output_data_frame = importer.import_file(output_path, version=version)

    assert output_data_frame.index.equals(geocoded_data_frame.index)
    for index, row in output_data_frame.iterrows():
        assert row['street'] == geocoded_data_frame.iloc[index]['street']


def test_add_line_comprehensive():
    file_path_1 = './tests/geocoding_cache_input_1.csv'
    file_path_2 = './tests/geocoding_cache_input_2.csv'
    output_path = './tests/comprehensive_geocoding_cache_output.csv'
    version = "comprehensive"
    input_data_frame_1 = importer.import_file(file_path_1, version=version)
    input_data_frame_2 = importer.import_file(file_path_2, version=version)

    geocoder.geocode_data_frame(input_data_frame_1.copy(), version=version)
    geocoded_data_frame = geocoder.geocode_data_frame(input_data_frame_2.copy(), version=version)

    output_data_frame = importer.import_file(output_path, version=version)

    assert output_data_frame.index.equals(geocoded_data_frame.index)
    for index, row in output_data_frame.iterrows():
        assert row['street'] == geocoded_data_frame.iloc[index]['street']
