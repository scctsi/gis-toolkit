from data_structure import DataElement, GetStrategy
from enum import Enum


class SedohDataSource(Enum):
    ACS = 1
    Gazetteer = 2
    CalEPA_CES = 3
    CDC = 4
    USDA = 5


class SedohDataElements:
    def __init__(self):
        self.data_elements = [
            # CDC data elements
            # Social Vulnerability Index
            DataElement(SedohDataSource.CDC,
                        "Social Vulnerability Index",
                        "social_vulnerability_index",
                        "RPL_THEMES",
                        GetStrategy.FILE),
            # ACS data elements
            # Gini Inequality Coefficient
            DataElement(SedohDataSource.ACS,
                        "Gini Inequality Coefficient",
                        "gini_inequality_coefficient",
                        "B19083_001E",
                        GetStrategy.PRIVATE_API),
            # Old Age Dependency Ratio
            DataElement(SedohDataSource.ACS,
                        "Old Age Dependency Ratio",
                        "old_age_dependency_ratio",
                        "S0101_C01_035E",
                        GetStrategy.PRIVATE_API),
            # Child Dependency Ratio
            DataElement(SedohDataSource.ACS,
                        "Child Dependency Ratio",
                        "child_dependency_ratio",
                        "S0101_C01_036E",
                        GetStrategy.PRIVATE_API),
            # Housing - Median Year Built
            DataElement(SedohDataSource.ACS,
                        "Housing - Median Year Built",
                        "housing_median_year_built", "B25035_001E",
                        GetStrategy.PRIVATE_API),
            # Housing - Percent Occupied Units Lacking Plumbing
            DataElement(SedohDataSource.ACS,
                        "Housing - Percent Occupied Units Lacking Plumbing",
                        "housing_percent_occupied_units_lacking_plumbing",
                        "S2504_C02_025E",
                        GetStrategy.CALCULATION),
            # Housing - Percent Occupied Units Lacking Complete Kitchen
            DataElement(SedohDataSource.ACS,
                        "Housing - Percent Occupied Units Lacking Complete Kitchen",
                        "housing_percent_occupied_lacking_complete_kitchen",
                        "S2504_C02_026E",
                        GetStrategy.CALCULATION),
            # Housing - Percent Occupied Units with No Bedroom
            DataElement(SedohDataSource.ACS,
                        "Housing - Percent Occupied Units with No Bedroom",
                        "housing_percent_occupied_units_with_no_bedroom",
                        "S2504_C02_021E",
                        GetStrategy.PRIVATE_API),
            # Housing - Percent Occupied Units with No Vehicle Available
            DataElement(SedohDataSource.ACS,
                        "Housing - Percent Occupied Units with No Vehicle Available",
                        "housing_percent_occupied_units_with_no_vehicle_available",
                        "S2504_C02_027E",
                        GetStrategy.PRIVATE_API),
            # Housing - Percent Occupied Units with No Computer [includes Smartphone]
            DataElement(SedohDataSource.ACS,
                        "Housing - Percent Occupied Units with No Computer [includes Smartphone]",
                        "housing_percent_occupied_units_with_no_computer_included_smartphone",
                        "S2801_C02_011E",
                        GetStrategy.PRIVATE_API),
            # Housing - Percent Occupied Units with No Internet Subscription
            DataElement(SedohDataSource.ACS,
                        "Housing - Percent Occupied Units with No Internet Subscription",
                        "housing_percent_occupied_units_with_no_internet_subscription",
                        "S0101_C01_036E",
                        GetStrategy.PRIVATE_API),
            # Population Density
            DataElement(SedohDataSource.ACS,
                        "Population Density",
                        "population_density",
                        "S0101_C01_001E",
                        GetStrategy.CALCULATION),
            # Percent Hispanic
            DataElement(SedohDataSource.ACS,
                        "Percent Hispanic",
                        "percent_hispanic",
                        "DP05_0071PE",
                        GetStrategy.PRIVATE_API),
            # Percent Non-Hispanic
            DataElement(SedohDataSource.ACS,
                        "Percent Non-Hispanic",
                        "percent_non_hispanic",
                        "DP05_0076PE",
                        GetStrategy.PRIVATE_API),
            # Percent American Indian or Alaska Native
            DataElement(SedohDataSource.ACS,
                        "Percent American Indian or Alaska Native",
                        "percent_american_indian_or_alaska_native",
                        "DP05_0039PE",
                        GetStrategy.PRIVATE_API),
            # Percent Asian
            DataElement(SedohDataSource.ACS,
                        "Percent Asian",
                        "percent_asian",
                        "DP05_0044PE",
                        GetStrategy.PRIVATE_API),
            # Percent Black
            DataElement(SedohDataSource.ACS,
                        "Percent Black",
                        "percent_black",
                        "DP05_0038PE",
                        GetStrategy.PRIVATE_API),
            # Percent Native Hawaiian or Other Pacific Islander
            DataElement(SedohDataSource.ACS,
                        "Percent Native Hawaiian or Other Pacific Islander",
                        "percent_native_hawaiian_or_other_pacific_islander",
                        "DP05_0052PE",
                        GetStrategy.PRIVATE_API),
            # Percent Multiple Race
            DataElement(SedohDataSource.ACS,
                        "Percent Multiple Race",
                        "percent_multiple_race",
                        "DP05_0058PE",
                        GetStrategy.PRIVATE_API),
            # Percent White
            DataElement(SedohDataSource.ACS,
                        "Percent White",
                        "percent_white",
                        "DP05_0037PE",
                        GetStrategy.PRIVATE_API),
            # Percent Some Other Race
            DataElement(SedohDataSource.ACS,
                        "Percent Some Other Race",
                        "percent_some_other_race",
                        "DP05_0057PE",
                        GetStrategy.PRIVATE_API),
            # Percent Below 100% of Fed Poverty Level
            DataElement(SedohDataSource.ACS,
                        "Percent Below 100% of Fed Poverty Level",
                        "percent_below_100_of_fed_poverty_level",
                        "S1701_C03_001E",
                        GetStrategy.PRIVATE_API),
            # Percent Below 200% of Fed Poverty Level
            DataElement(SedohDataSource.ACS,
                        "Percent Below 200% of Fed Poverty Level",
                        "percent_below_200_of_fed_poverty_level",
                        "...",
                        GetStrategy.CALCULATION),
            # Percent Below 300% of Fed Poverty Level
            DataElement(SedohDataSource.ACS,
                        "Percent Below 300% of Fed Poverty Level",
                        "percent_below_300_of_fed_poverty_level",
                        "...",
                        GetStrategy.CALCULATION),
            # Percent Households that Receive SNAP
            DataElement(SedohDataSource.ACS,
                        "Percent Households that Receive SNAP",
                        "percent_households_that_receive_snap",
                        "S2201_C04_001E",
                        GetStrategy.PRIVATE_API),
            # Percent Households with Limited English
            DataElement(SedohDataSource.ACS,
                        "Percent Households with Limited English",
                        "percent_households_with_limited_english",
                        "S1602_C04_001E",
                        GetStrategy.PRIVATE_API),
            # Percent High School Grad - Age 25 or Over
            DataElement(SedohDataSource.ACS,
                        "Percent High School Grad - Age 25 or Over",
                        "percent_households_with_limited_english",
                        "S1501_C02_009E",
                        GetStrategy.PRIVATE_API),
            # Percent Bachelor's Degree - Age 25 or Over
            DataElement(SedohDataSource.ACS,
                        "Percent Bachelor's Degree - Age 25 or Over",
                        "percent_bachelors_degree_age_25_or_over",
                        "S1501_C01_012E",
                        GetStrategy.PRIVATE_API),
            # Median Household Income
            DataElement(SedohDataSource.ACS,
                        "Median Household Income",
                        "median_household_income",
                        "B19013_001E",
                        GetStrategy.PRIVATE_API),
            # Unemployment Rate - Age 16 or Over
            DataElement(SedohDataSource.ACS,
                        "Unemployment Rate - Age 16 or Over",
                        "unemployment_rate_age_16_or_over",
                        "DP03_0005PE",
                        GetStrategy.PRIVATE_API),
            # CalEPA_CES data elements
            # Air Quality Indicator - Ozone (O3)
            DataElement(SedohDataSource.CalEPA_CES,
                        "Air Quality Indicator - Ozone (O3)",
                        "air_quality_indicator_ozone_o3",
                        "Ozone",
                        GetStrategy.FILE),
            DataElement(SedohDataSource.CalEPA_CES,
                        "Air Quality Indicator - PM2.5",
                        "air_quality_indicator_pm25",
                        "PM2.5",
                        GetStrategy.FILE),
            DataElement(SedohDataSource.CalEPA_CES,
                        "Drinking Water Quality Indicator",
                        "drinking_water_quality_indicator",
                        "Drinking Water",
                        GetStrategy.FILE),
            DataElement(SedohDataSource.CalEPA_CES,
                        "Air Quality Indicator - Asthma ER Visits",
                        "air_quality_indicator_asthma_er_visits",
                        "Asthma",
                        GetStrategy.FILE),
            # USDA data elements
            # Food - Fraction of Population with Low Access
            DataElement(SedohDataSource.USDA,
                        "Food - Fraction of Population with Low Access",
                        "food_fraction_of_population_with_low_access",
                        ["Urban", "lapop1shar", "lapop10sha"],
                        GetStrategy.FILE_AND_CALCULATION),
            DataElement(SedohDataSource.USDA,
                        "Food - Low-Access Tract",
                        "food_low_access_tract",
                        "LA1and10",
                        GetStrategy.FILE),
        ]

    def filtered_data_elements(self, data_source):
        filtered_data_elements = \
            filter(lambda data_element: data_element.data_source == data_source, self.data_elements)

        return(list(filtered_data_elements))
