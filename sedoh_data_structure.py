from data_structure import DataElement, DataSource, RasterSource, ACSSource, GetStrategy, NasaSource
from enum import Enum
from datetime import datetime
import json


class SedohDataSource(Enum):
    ACS = 1
    Gazetteer = 2
    CalEPA_CES = 3
    CDC = 4
    USDA = 5
    SCEHSC = 6
    NASA = 7


class DataFiles:
    def __init__(self):
        with open('./data_files_key.json') as save_file:
            data = json.load(save_file)
        self.data_files = {}
        for source in data:
            if data[source]["type"] == "ACS":
                data_sources = []
                for version in data[source]["versions"]:
                    data_sources.append(ACSSource(
                        acs_year=version["acs_year"],
                        start_date=datetime.strptime(version["valid_start_date"], "%Y-%m-%dT%H:%M:%S.%fZ"),
                        end_date=datetime.strptime(version["valid_end_date"], "%Y-%m-%dT%H:%M:%S.%fZ")
                    ))
                self.data_files.update({vars(SedohDataSource)["_member_map_"][source]: data_sources})
            elif data[source]["type"] == "Census":
                data_sources = []
                for version in data[source]["versions"]:
                    data_sources.append(DataSource(
                        file_name=version["file_path"],
                        tract_column=version["census_tract_column_name"],
                        start_date=datetime.strptime(version["valid_start_date"], "%Y-%m-%dT%H:%M:%S.%fZ"),
                        end_date=datetime.strptime(version["valid_end_date"], "%Y-%m-%dT%H:%M:%S.%fZ")
                    ))
                self.data_files.update({vars(SedohDataSource)["_member_map_"][source]: data_sources})
            elif data[source]["type"] == "Raster":
                pollutants = [key for key in data[source] if key != "type"]
                for pollutant in pollutants:
                    data_sources = []
                    for version in data[source][pollutant]["versions"]:
                        data_sources.append(RasterSource(
                            file_name=version["file_path"],
                            latitude_range=version["latitude_range"],
                            longitude_range=version["longitude_range"],
                            precision=version["precision"],
                            start_date=datetime.strptime(version["valid_start_date"], "%Y-%m-%dT%H:%M:%S.%fZ"),
                            end_date=datetime.strptime(version["valid_end_date"], "%Y-%m-%dT%H:%M:%S.%fZ")
                        ))
                    self.data_files.update({(vars(SedohDataSource)["_member_map_"][source], pollutant): data_sources})
            elif data[source]["type"] == "Nasa":
                pollutants = [key for key in data[source] if key != "type"]
                for pollutant in pollutants:
                    data_sources = []
                    for version in data[source][pollutant]["versions"]:
                        data_sources.append(NasaSource(
                            file_name=version["file_path"],
                            start_date=datetime.strptime(version["valid_start_date"], "%Y-%m-%dT%H:%M:%S.%fZ"),
                            end_date=datetime.strptime(version["valid_end_date"], "%Y-%m-%dT%H:%M:%S.%fZ")
                        ))
                    self.data_files.update({(vars(SedohDataSource)["_member_map_"][source], pollutant): data_sources})


class SedohDataElements:
    def __init__(self):
        self.data_elements = [
            # CDC data elements
            # Social Vulnerability Index
            DataElement(SedohDataSource.CDC,
                        "Social Vulnerability Index",
                        "social_vulnerability_index",
                        "RPL_THEMES",
                        GetStrategy.FILE,
                        "SVI"),
            # ACS data elements
            # Gini Inequality Coefficient
            DataElement(SedohDataSource.ACS,
                        "Gini Inequality Coefficient",
                        "gini_inequality_coefficient",
                        "B19083_001E",
                        GetStrategy.PRIVATE_API,
                        "GINI"),
            # Old Age Dependency Ratio
            DataElement(SedohDataSource.ACS,
                        "Old Age Dependency Ratio",
                        "old_age_dependency_ratio",
                        "S0101_C01_035E",
                        GetStrategy.PRIVATE_API,
                        "POP_OLD"),
            # Child Dependency Ratio
            DataElement(SedohDataSource.ACS,
                        "Child Dependency Ratio",
                        "child_dependency_ratio",
                        "S0101_C01_036E",
                        GetStrategy.PRIVATE_API,
                        "POP_CHILD"),
            # Housing - Median Year Built
            DataElement(SedohDataSource.ACS,
                        "Housing - Median Year Built",
                        "housing_median_year_built", "B25035_001E",
                        GetStrategy.PRIVATE_API,
                        "HU_AGE"),
            # Housing - Percent Occupied Units Lacking Plumbing
            DataElement(SedohDataSource.ACS,
                        "Housing - Percent Occupied Units Lacking Plumbing",
                        "housing_percent_occupied_units_lacking_plumbing",
                        "S2504_C02_025E",
                        GetStrategy.CALCULATION,
                        "HU_PLUMBING"),
            # Housing - Percent Occupied Units Lacking Complete Kitchen
            DataElement(SedohDataSource.ACS,
                        "Housing - Percent Occupied Units Lacking Complete Kitchen",
                        "housing_percent_occupied_lacking_complete_kitchen",
                        "S2504_C02_026E",
                        GetStrategy.CALCULATION,
                        "HU_KITCHEN"),
            # Housing - Percent Occupied Units with No Bedroom
            DataElement(SedohDataSource.ACS,
                        "Housing - Percent Occupied Units with No Bedroom",
                        "housing_percent_occupied_units_with_no_bedroom",
                        "S2504_C02_021E",
                        GetStrategy.PRIVATE_API,
                        "HU_BEDROOM"),
            # Housing - Percent Occupied Units with No Vehicle Available
            DataElement(SedohDataSource.ACS,
                        "Housing - Percent Occupied Units with No Vehicle Available",
                        "housing_percent_occupied_units_with_no_vehicle_available",
                        "S2504_C02_027E",
                        GetStrategy.PRIVATE_API,
                        "HU_VEHICLE"),
            # Housing - Percent Occupied Units with No Computer [includes Smartphone]
            DataElement(SedohDataSource.ACS,
                        "Housing - Percent Occupied Units with No Computer [includes Smartphone]",
                        "housing_percent_occupied_units_with_no_computer_included_smartphone",
                        "S2801_C02_011E",
                        GetStrategy.PRIVATE_API,
                        "HU_COMPUTER"),
            # Housing - Percent Occupied Units with No Internet Subscription
            DataElement(SedohDataSource.ACS,
                        "Housing - Percent Occupied Units with No Internet Subscription",
                        "housing_percent_occupied_units_with_no_internet_subscription",
                        "S2801_C02_019E",
                        GetStrategy.PRIVATE_API,
                        "HU_INTERNET"),
            # Population Density
            DataElement(SedohDataSource.ACS,
                        "Population Density",
                        "population_density",
                        "S0101_C01_001E",
                        GetStrategy.CALCULATION,
                        "POP_DEN"),
            # Percent Hispanic
            DataElement(SedohDataSource.ACS,
                        "Percent Hispanic",
                        "percent_hispanic",
                        "DP05_0071PE",
                        GetStrategy.PRIVATE_API,
                        "POP_HISPANIC"),
            # Percent Non-Hispanic
            DataElement(SedohDataSource.ACS,
                        "Percent Non-Hispanic",
                        "percent_non_hispanic",
                        "DP05_0076PE",
                        GetStrategy.PRIVATE_API,
                        "POP_NON_HISPANIC"),
            # Percent American Indian or Alaska Native
            DataElement(SedohDataSource.ACS,
                        "Percent American Indian or Alaska Native",
                        "percent_american_indian_or_alaska_native",
                        "DP05_0039PE",
                        GetStrategy.PRIVATE_API,
                        "POP_NATIVE_AMERICAN"),
            # Percent Asian
            DataElement(SedohDataSource.ACS,
                        "Percent Asian",
                        "percent_asian",
                        "DP05_0044PE",
                        GetStrategy.PRIVATE_API,
                        "POP_ASIAN"),
            # Percent Black
            DataElement(SedohDataSource.ACS,
                        "Percent Black",
                        "percent_black",
                        "DP05_0038PE",
                        GetStrategy.PRIVATE_API,
                        "POP_BLACK"),
            # Percent Native Hawaiian or Other Pacific Islander
            DataElement(SedohDataSource.ACS,
                        "Percent Native Hawaiian or Other Pacific Islander",
                        "percent_native_hawaiian_or_other_pacific_islander",
                        "DP05_0052PE",
                        GetStrategy.PRIVATE_API,
                        "POP_PACIFIC_ISLANDER"),
            # Percent Multiple Race
            DataElement(SedohDataSource.ACS,
                        "Percent Multiple Race",
                        "percent_multiple_race",
                        "DP05_0058PE",
                        GetStrategy.PRIVATE_API,
                        "POP_MULTIPLE_RACE"),
            # Percent White
            DataElement(SedohDataSource.ACS,
                        "Percent White",
                        "percent_white",
                        "DP05_0037PE",
                        GetStrategy.PRIVATE_API,
                        "POP_WHITE"),
            # Percent Some Other Race
            DataElement(SedohDataSource.ACS,
                        "Percent Some Other Race",
                        "percent_some_other_race",
                        "DP05_0057PE",
                        GetStrategy.PRIVATE_API,
                        "POP_OTHER_RACE"),
            # Percent Below 100% of Fed Poverty Level
            DataElement(SedohDataSource.ACS,
                        "Percent Below 100% of Fed Poverty Level",
                        "percent_below_100_of_fed_poverty_level",
                        "S1701_C03_001E",
                        GetStrategy.PRIVATE_API,
                        "INCOME_100FPL"),
            # Percent Below 200% of Fed Poverty Level
            DataElement(SedohDataSource.ACS,
                        "Percent Below 200% of Fed Poverty Level",
                        "percent_below_200_of_fed_poverty_level",
                        "S1701_C01_042E,S1701_C01_001E",
                        GetStrategy.CALCULATION,
                        "INCOME_200FPL"),
            # Percent Below 300% of Fed Poverty Level
            DataElement(SedohDataSource.ACS,
                        "Percent Below 300% of Fed Poverty Level",
                        "percent_below_300_of_fed_poverty_level",
                        "S1701_C01_043E,S1701_C01_001E",
                        GetStrategy.CALCULATION,
                        "INCOME_300FPL"),
            # Percent Households that Receive SNAP
            DataElement(SedohDataSource.ACS,
                        "Percent Households that Receive SNAP",
                        "percent_households_that_receive_snap",
                        "S2201_C04_001E",
                        GetStrategy.CALCULATION,
                        "FOOD_SNAP"),
            # Percent Households with Limited English
            DataElement(SedohDataSource.ACS,
                        "Percent Households with Limited English",
                        "percent_households_with_limited_english",
                        "S1602_C04_001E",
                        GetStrategy.PRIVATE_API,
                        "LIMITED_ENGLISH"),
            # Percent High School Grad - Age 25 or Over
            DataElement(SedohDataSource.ACS,
                        "Percent High School Grad - Age 25 or Over",
                        "percent_high_school_grad_age_25_or_over",
                        "S1501_C02_009E",
                        GetStrategy.PRIVATE_API,
                        "EDUCATION_HIGH_SCHOOL"),
            # Percent Bachelor's Degree - Age 25 or Over
            DataElement(SedohDataSource.ACS,
                        "Percent Bachelor's Degree - Age 25 or Over",
                        "percent_bachelors_degree_age_25_or_over",
                        "S1501_C02_012E",
                        GetStrategy.PRIVATE_API,
                        "EDUCATION_BACHELORS"),
            # Median Household Income
            DataElement(SedohDataSource.ACS,
                        "Median Household Income",
                        "median_household_income",
                        "B19013_001E",
                        GetStrategy.PRIVATE_API,
                        "INCOME_MHH"),
            # Unemployment Rate - Age 16 or Over
            DataElement(SedohDataSource.ACS,
                        "Unemployment Rate - Age 16 or Over",
                        "unemployment_rate_age_16_or_over",
                        "DP03_0005PE",
                        GetStrategy.PRIVATE_API,
                        "UNEMPLOYMENT"),
            # CalEPA_CES data elements
            # Air Quality Indicator - Ozone (O3)
            DataElement(SedohDataSource.CalEPA_CES,
                        "Air Quality Indicator - Ozone (O3)",
                        "air_quality_indicator_ozone_o3",
                        "Ozone",
                        GetStrategy.FILE,
                        "OZONE_CES"),
            DataElement(SedohDataSource.CalEPA_CES,
                        "Air Quality Indicator - PM2.5",
                        "air_quality_indicator_pm25",
                        "PM2.5",
                        GetStrategy.FILE,
                        "PM_2.5_CES"),
            DataElement(SedohDataSource.CalEPA_CES,
                        "Drinking Water Quality Indicator",
                        "drinking_water_quality_indicator",
                        "Drinking Water",
                        GetStrategy.FILE,
                        "DRINKING_WATER"),
            DataElement(SedohDataSource.CalEPA_CES,
                        "Air Quality Indicator - Asthma ER Visits",
                        "air_quality_indicator_asthma_er_visits",
                        "Asthma",
                        GetStrategy.FILE,
                        "ASTHMA"),
            # USDA data elements
            # Food - Fraction of Population with Low Access
            DataElement(SedohDataSource.USDA,
                        "Food - Fraction of Population with Low Access",
                        "food_fraction_of_population_with_low_access",
                        ["Urban", "lapop1share", "lapop10share"],
                        GetStrategy.FILE_AND_CALCULATION,
                        "FOOD_LOW_ACCESS"),
            DataElement(SedohDataSource.USDA,
                        "Food - Low-Access Tract",
                        "food_low_access_tract",
                        "LA1and10",
                        GetStrategy.FILE,
                        "FOOD_LOW_ACCESS_TRACT"),
            DataElement((SedohDataSource.SCEHSC, "O3"),
                        "Annual Pollutant Data from SCEHSC - Ozone",
                        "scehsc_annual_pollutant_ozone_ppb",
                        "O3",
                        GetStrategy.RASTER_FILE,
                        "OZONE"),
            DataElement((SedohDataSource.SCEHSC, "NO2"),
                        "Annual Pollutant Data from SCEHSC - Nitrogen Dioxide",
                        "scehsc_annual_pollutant_nitrogen_dioxide_ppb",
                        "NO2",
                        GetStrategy.RASTER_FILE,
                        "NITROGEN DIOXIDE"),
            DataElement((SedohDataSource.SCEHSC, "PM25"),
                        "Annual Pollutant Data from SCEHSC - PM2.5",
                        "scehsc_annual_pollutant_pm25_ug/m^3",
                        "PM25",
                        GetStrategy.RASTER_FILE,
                        "PM_2.5"),
            DataElement((SedohDataSource.SCEHSC, "PM10"),
                        "Annual Pollutant Data from SCEHSC - PM10",
                        "scehsc_annual_pollutant_pm10_ug/m^3",
                        "PM10",
                        GetStrategy.RASTER_FILE,
                        "PM_10"),
            DataElement((SedohDataSource.NASA, "O3"),
                        "Annual Pollutant Data from Nasa - O3",
                        "nasa_annual_pollutant_ozone_ppb",
                        "O3",
                        GetStrategy.RASTER_FILE,
                        "NASA_O3"),
            DataElement((SedohDataSource.NASA, "PM25"),
                        "Annual Pollutant Data from Nasa - PM2.5",
                        "nasa_annual_pollutant_pm25_ug/m^3",
                        "PM25",
                        GetStrategy.RASTER_FILE,
                        "NASA_PM_2.5")
        ]

    def filtered_data_elements(self, data_source, get_strategy=None):
        if not (get_strategy is None):
            filtered_data_elements = \
                filter(lambda data_element: data_element.data_source == data_source and
                                            data_element.get_strategy == get_strategy, self.data_elements)
        else:
            filtered_data_elements = \
                filter(lambda data_element: data_element.data_source == data_source, self.data_elements)

        return list(filtered_data_elements)
