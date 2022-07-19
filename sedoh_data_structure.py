from data_structure import DataElement, DataSource, RasterSource, ACSSource, GetStrategy, NasaSource
from enum import Enum
from datetime import datetime


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
        self.data_files = {
            # ACS data sets overlap, where each year is valid for up to 5 years before it, so the middle year is
            # chosen as the median to represent the data of those 5 years (ex. acs query for "2012" is valid for the
            # range 2008-2012, so the middle year, 2010, will be the time range for this query)
            SedohDataSource.ACS: [
                ACSSource("2012",
                          datetime(2010, 1, 1),
                          datetime(2010, 12, 31)),
                ACSSource("2013",
                          datetime(2011, 1, 1),
                          datetime(2011, 12, 31)),
                ACSSource("2014",
                          datetime(2012, 1, 1),
                          datetime(2012, 12, 31)),
                ACSSource("2015",
                          datetime(2013, 1, 1),
                          datetime(2013, 12, 31)),
                ACSSource("2016",
                          datetime(2014, 1, 1),
                          datetime(2014, 12, 31)),
                ACSSource("2017",
                          datetime(2015, 1, 1),
                          datetime(2015, 12, 31)),
                ACSSource("2018",
                          datetime(2016, 1, 1),
                          datetime(2016, 12, 31)),
                ACSSource("2019",
                          datetime(2017, 1, 1),
                          datetime(2017, 12, 31)),
                ACSSource("2020",
                          datetime(2018, 1, 1),
                          datetime(2018, 12, 31))
            ],
        }
            # # The following data sets include the data used for file-based variables. These data sets do not overlap
            # SedohDataSource.CalEPA_CES: [
            #     DataSource("calepa_ces/calepa_ces_2.0.csv",
            #                "Census Tract",
            #                datetime(2014, 10, 1),
            #                datetime(2018, 5, 31)),
            #     DataSource("calepa_ces/calepa_ces_3.0.csv",
            #                "Census Tract",
            #                datetime(2018, 6, 1),
            #                datetime(2021, 10, 12)),
            #     DataSource("calepa_ces/calepa_ces_4.0.csv",
            #                "Census Tract",
            #                datetime(2021, 10, 13),
            #                datetime(2024, 12, 31))
            # ],
            # SedohDataSource.CDC: [
            #     DataSource("cdc/cdc_2000.csv",
            #                "FIPS",
            #                datetime(2000, 1, 1),
            #                datetime(2009, 12, 31)),
            #     DataSource("cdc/cdc_2010.csv",
            #                "FIPS",
            #                datetime(2010, 1, 1),
            #                datetime(2013, 12, 31)),
            #     DataSource("cdc/cdc_2014.csv",
            #                "FIPS",
            #                datetime(2014, 1, 1),
            #                datetime(2015, 12, 31)),
            #     DataSource("cdc/cdc_2016.csv",
            #                "FIPS",
            #                datetime(2016, 1, 1),
            #                datetime(2017, 12, 31)),
            #     DataSource("cdc/cdc_2018.csv",
            #                "FIPS",
            #                datetime(2018, 1, 1),
            #                datetime(2019, 12, 31))
            # ],
            # SedohDataSource.Gazetteer: [
            #     DataSource("gazetteer/gazetteer_2010.txt",
            #                "GEOID",
            #                datetime(2001, 1, 1),
            #                datetime(2010, 12, 31)),
            #     DataSource("gazetteer/gazetteer_2020.txt",
            #                "GEOID",
            #                datetime(2011, 1, 1),
            #                datetime(2020, 12, 31))
            # ],
            # SedohDataSource.USDA: [
            #     DataSource("usda/usda_2010.csv",
            #                "CensusTract",
            #                datetime(2010, 1, 1),
            #                datetime(2014, 12, 31)),
            #     DataSource("usda/usda_2015.csv",
            #                "CensusTract",
            #                datetime(2015, 1, 1),
            #                datetime(2018, 12, 31)),
            #     DataSource("usda/usda_2019.csv",
            #                "CensusTract",
            #                datetime(2019, 1, 1),
            #                datetime(2024, 12, 31))
            # ],
            # The remaining data sets are for geographic pollutant data which are read from raster files.
        #     (SedohDataSource.SCEHSC, "NO2"): [
        #         RasterSource("scehsc/NO2/NO2_1998_ANN.tif",
        #                      (32.5, 35.5),
        #                      (-121.05, -114.1),
        #                      2,
        #                      datetime(1998, 1, 1),
        #                      datetime(1998, 12, 31)),
        #         RasterSource("scehsc/NO2/NO2_1999_ANN.tif",
        #                      (32.5, 35.5),
        #                      (-121.05, -114.1),
        #                      2,
        #                      datetime(1999, 1, 1),
        #                      datetime(1999, 12, 31)),
        #         RasterSource("scehsc/NO2/NO2_2000_ANN.tif",
        #                      (32.5, 35.5),
        #                      (-121.05, -114.1),
        #                      2,
        #                      datetime(2000, 1, 1),
        #                      datetime(2000, 12, 31)),
        #         RasterSource("scehsc/NO2/NO2_2001_ANN.tif",
        #                      (32.5, 35.5),
        #                      (-121.05, -114.1),
        #                      2,
        #                      datetime(2001, 1, 1),
        #                      datetime(2001, 12, 31)),
        #         RasterSource("scehsc/NO2/NO2_2002_ANN.tif",
        #                      (32.5, 35.5),
        #                      (-121.05, -114.1),
        #                      2,
        #                      datetime(2002, 1, 1),
        #                      datetime(2002, 12, 31)),
        #         RasterSource("scehsc/NO2/NO2_2003_ANN.tif",
        #                      (32.5, 35.5),
        #                      (-121.05, -114.1),
        #                      2,
        #                      datetime(2003, 1, 1),
        #                      datetime(2003, 12, 31)),
        #         RasterSource("scehsc/NO2/NO2_2004_ANN.tif",
        #                      (32.5, 35.5),
        #                      (-121.05, -114.1),
        #                      2,
        #                      datetime(2004, 1, 1),
        #                      datetime(2004, 12, 31)),
        #         RasterSource("scehsc/NO2/NO2_2005_ANN.tif",
        #                      (32.5, 35.5),
        #                      (-121.05, -114.1),
        #                      2,
        #                      datetime(2005, 1, 1),
        #                      datetime(2005, 12, 31)),
        #         RasterSource("scehsc/NO2/NO2_2006_ANN.tif",
        #                      (32.5, 35.5),
        #                      (-121.05, -114.1),
        #                      2,
        #                      datetime(2006, 1, 1),
        #                      datetime(2006, 12, 31)),
        #         RasterSource("scehsc/NO2/NO2_2007_ANN.tif",
        #                      (32.5, 35.5),
        #                      (-121.05, -114.1),
        #                      2,
        #                      datetime(2007, 1, 1),
        #                      datetime(2007, 12, 31)),
        #         RasterSource("scehsc/NO2/NO2_2008_ANN.tif",
        #                      (32.5, 35.5),
        #                      (-121.05, -114.1),
        #                      2,
        #                      datetime(2008, 1, 1),
        #                      datetime(2008, 12, 31)),
        #         RasterSource("scehsc/NO2/NO2_2009_ANN.tif",
        #                      (32.5, 35.5),
        #                      (-121.05, -114.1),
        #                      2,
        #                      datetime(2009, 1, 1),
        #                      datetime(2009, 12, 31))
        #     ],
        #     (SedohDataSource.SCEHSC, "O3"): [
        #         RasterSource("scehsc/O3/O3_1998_ANN.tif",
        #                      (32.5, 35.5),
        #                      (-121.05, -114.1),
        #                      2,
        #                      datetime(1998, 1, 1),
        #                      datetime(1998, 12, 31)),
        #         RasterSource("scehsc/O3/O3_1999_ANN.tif",
        #                      (32.5, 35.5),
        #                      (-121.05, -114.1),
        #                      2,
        #                      datetime(1999, 1, 1),
        #                      datetime(1999, 12, 31)),
        #         RasterSource("scehsc/O3/O3_2000_ANN.tif",
        #                      (32.5, 35.5),
        #                      (-121.05, -114.1),
        #                      2,
        #                      datetime(2000, 1, 1),
        #                      datetime(2000, 12, 31)),
        #         RasterSource("scehsc/O3/O3_2001_ANN.tif",
        #                      (32.5, 35.5),
        #                      (-121.05, -114.1),
        #                      2,
        #                      datetime(2001, 1, 1),
        #                      datetime(2001, 12, 31)),
        #         RasterSource("scehsc/O3/O3_2002_ANN.tif",
        #                      (32.5, 35.5),
        #                      (-121.05, -114.1),
        #                      2,
        #                      datetime(2002, 1, 1),
        #                      datetime(2002, 12, 31)),
        #         RasterSource("scehsc/O3/O3_2003_ANN.tif",
        #                      (32.5, 35.5),
        #                      (-121.05, -114.1),
        #                      2,
        #                      datetime(2003, 1, 1),
        #                      datetime(2003, 12, 31)),
        #         RasterSource("scehsc/O3/O3_2004_ANN.tif",
        #                      (32.5, 35.5),
        #                      (-121.05, -114.1),
        #                      2,
        #                      datetime(2004, 1, 1),
        #                      datetime(2004, 12, 31)),
        #         RasterSource("scehsc/O3/O3_2005_ANN.tif",
        #                      (32.5, 35.5),
        #                      (-121.05, -114.1),
        #                      2,
        #                      datetime(2005, 1, 1),
        #                      datetime(2005, 12, 31)),
        #         RasterSource("scehsc/O3/O3_2006_ANN.tif",
        #                      (32.5, 35.5),
        #                      (-121.05, -114.1),
        #                      2,
        #                      datetime(2006, 1, 1),
        #                      datetime(2006, 12, 31)),
        #         RasterSource("scehsc/O3/O3_2007_ANN.tif",
        #                      (32.5, 35.5),
        #                      (-121.05, -114.1),
        #                      2,
        #                      datetime(2007, 1, 1),
        #                      datetime(2007, 12, 31)),
        #         RasterSource("scehsc/O3/O3_2008_ANN.tif",
        #                      (32.5, 35.5),
        #                      (-121.05, -114.1),
        #                      2,
        #                      datetime(2008, 1, 1),
        #                      datetime(2008, 12, 31)),
        #         RasterSource("scehsc/O3/O3_2009_ANN.tif",
        #                      (32.5, 35.5),
        #                      (-121.05, -114.1),
        #                      2,
        #                      datetime(2009, 1, 1),
        #                      datetime(2009, 12, 31))
        #     ],
        #     (SedohDataSource.SCEHSC, "PM10"): [
        #         RasterSource("scehsc/PM10/PM10_1998_ANN.tif",
        #                      (32.5, 35.5),
        #                      (-121.05, -114.1),
        #                      2,
        #                      datetime(1998, 1, 1),
        #                      datetime(1998, 12, 31)),
        #         RasterSource("scehsc/PM10/PM10_1999_ANN.tif",
        #                      (32.5, 35.5),
        #                      (-121.05, -114.1),
        #                      2,
        #                      datetime(1999, 1, 1),
        #                      datetime(1999, 12, 31)),
        #         RasterSource("scehsc/PM10/PM10_2000_ANN.tif",
        #                      (32.5, 35.5),
        #                      (-121.05, -114.1),
        #                      2,
        #                      datetime(2000, 1, 1),
        #                      datetime(2000, 12, 31)),
        #         RasterSource("scehsc/PM10/PM10_2001_ANN.tif",
        #                      (32.5, 35.5),
        #                      (-121.05, -114.1),
        #                      2,
        #                      datetime(2001, 1, 1),
        #                      datetime(2001, 12, 31)),
        #         RasterSource("scehsc/PM10/PM10_2002_ANN.tif",
        #                      (32.5, 35.5),
        #                      (-121.05, -114.1),
        #                      2,
        #                      datetime(2002, 1, 1),
        #                      datetime(2002, 12, 31)),
        #         RasterSource("scehsc/PM10/PM10_2003_ANN.tif",
        #                      (32.5, 35.5),
        #                      (-121.05, -114.1),
        #                      2,
        #                      datetime(2003, 1, 1),
        #                      datetime(2003, 12, 31)),
        #         RasterSource("scehsc/PM10/PM10_2004_ANN.tif",
        #                      (32.5, 35.5),
        #                      (-121.05, -114.1),
        #                      2,
        #                      datetime(2004, 1, 1),
        #                      datetime(2004, 12, 31)),
        #         RasterSource("scehsc/PM10/PM10_2005_ANN.tif",
        #                      (32.5, 35.5),
        #                      (-121.05, -114.1),
        #                      2,
        #                      datetime(2005, 1, 1),
        #                      datetime(2005, 12, 31)),
        #         RasterSource("scehsc/PM10/PM10_2006_ANN.tif",
        #                      (32.5, 35.5),
        #                      (-121.05, -114.1),
        #                      2,
        #                      datetime(2006, 1, 1),
        #                      datetime(2006, 12, 31)),
        #         RasterSource("scehsc/PM10/PM10_2007_ANN.tif",
        #                      (32.5, 35.5),
        #                      (-121.05, -114.1),
        #                      2,
        #                      datetime(2007, 1, 1),
        #                      datetime(2007, 12, 31)),
        #         RasterSource("scehsc/PM10/PM10_2008_ANN.tif",
        #                      (32.5, 35.5),
        #                      (-121.05, -114.1),
        #                      2,
        #                      datetime(2008, 1, 1),
        #                      datetime(2008, 12, 31)),
        #         RasterSource("scehsc/PM10/PM10_2009_ANN.tif",
        #                      (32.5, 35.5),
        #                      (-121.05, -114.1),
        #                      2,
        #                      datetime(2009, 1, 1),
        #                      datetime(2009, 12, 31))
        #     ],
        #     (SedohDataSource.SCEHSC, "PM25"): [
        #         RasterSource("scehsc/PM25/PM25_1998_ANN.tif",
        #                      (32.5, 35.5),
        #                      (-121.05, -114.1),
        #                      2,
        #                      datetime(1998, 1, 1),
        #                      datetime(1998, 12, 31)),
        #         RasterSource("scehsc/PM25/PM25_1999_ANN.tif",
        #                      (32.5, 35.5),
        #                      (-121.05, -114.1),
        #                      2,
        #                      datetime(1999, 1, 1),
        #                      datetime(1999, 12, 31)),
        #         RasterSource("scehsc/PM25/PM25_2000_ANN.tif",
        #                      (32.5, 35.5),
        #                      (-121.05, -114.1),
        #                      2,
        #                      datetime(2000, 1, 1),
        #                      datetime(2000, 12, 31)),
        #         RasterSource("scehsc/PM25/PM25_2001_ANN.tif",
        #                      (32.5, 35.5),
        #                      (-121.05, -114.1),
        #                      2,
        #                      datetime(2001, 1, 1),
        #                      datetime(2001, 12, 31)),
        #         RasterSource("scehsc/PM25/PM25_2002_ANN.tif",
        #                      (32.5, 35.5),
        #                      (-121.05, -114.1),
        #                      2,
        #                      datetime(2002, 1, 1),
        #                      datetime(2002, 12, 31)),
        #         RasterSource("scehsc/PM25/PM25_2003_ANN.tif",
        #                      (32.5, 35.5),
        #                      (-121.05, -114.1),
        #                      2,
        #                      datetime(2003, 1, 1),
        #                      datetime(2003, 12, 31)),
        #         RasterSource("scehsc/PM25/PM25_2004_ANN.tif",
        #                      (32.5, 35.5),
        #                      (-121.05, -114.1),
        #                      2,
        #                      datetime(2004, 1, 1),
        #                      datetime(2004, 12, 31)),
        #         RasterSource("scehsc/PM25/PM25_2005_ANN.tif",
        #                      (32.5, 35.5),
        #                      (-121.05, -114.1),
        #                      2,
        #                      datetime(2005, 1, 1),
        #                      datetime(2005, 12, 31)),
        #         RasterSource("scehsc/PM25/PM25_2006_ANN.tif",
        #                      (32.5, 35.5),
        #                      (-121.05, -114.1),
        #                      2,
        #                      datetime(2006, 1, 1),
        #                      datetime(2006, 12, 31)),
        #         RasterSource("scehsc/PM25/PM25_2007_ANN.tif",
        #                      (32.5, 35.5),
        #                      (-121.05, -114.1),
        #                      2,
        #                      datetime(2007, 1, 1),
        #                      datetime(2007, 12, 31)),
        #         RasterSource("scehsc/PM25/PM25_2008_ANN.tif",
        #                      (32.5, 35.5),
        #                      (-121.05, -114.1),
        #                      2,
        #                      datetime(2008, 1, 1),
        #                      datetime(2008, 12, 31)),
        #         RasterSource("scehsc/PM25/PM25_2009_ANN.tif",
        #                      (32.5, 35.5),
        #                      (-121.05, -114.1),
        #                      2,
        #                      datetime(2009, 1, 1),
        #                      datetime(2009, 12, 31))
        #     ],
        #     (SedohDataSource.NASA, 'PM25'): [
        #         NasaSource("PM25/2000.tif",
        #                    datetime(2000, 1, 1),
        #                    datetime(2000, 12, 31)),
        #         NasaSource("PM25/2001.tif",
        #                    datetime(2001, 1, 1),
        #                    datetime(2001, 12, 31)),
        #         NasaSource("PM25/2002.tif",
        #                    datetime(2002, 1, 1),
        #                    datetime(2002, 12, 31)),
        #         NasaSource("PM25/2003.tif",
        #                    datetime(2003, 1, 1),
        #                    datetime(2003, 12, 31)),
        #         NasaSource("PM25/2004.tif",
        #                    datetime(2004, 1, 1),
        #                    datetime(2004, 12, 31)),
        #         NasaSource("PM25/2005.tif",
        #                    datetime(2005, 1, 1),
        #                    datetime(2005, 12, 31)),
        #         NasaSource("PM25/2006.tif",
        #                    datetime(2006, 1, 1),
        #                    datetime(2006, 12, 31)),
        #         NasaSource("PM25/2007.tif",
        #                    datetime(2007, 1, 1),
        #                    datetime(2007, 12, 31)),
        #         NasaSource("PM25/2008.tif",
        #                    datetime(2008, 1, 1),
        #                    datetime(2008, 12, 31)),
        #         NasaSource("PM25/2009.tif",
        #                    datetime(2009, 1, 1),
        #                    datetime(2009, 12, 31)),
        #         NasaSource("PM25/2010.tif",
        #                    datetime(2010, 1, 1),
        #                    datetime(2010, 12, 31)),
        #         NasaSource("PM25/2011.tif",
        #                    datetime(2011, 1, 1),
        #                    datetime(2011, 12, 31)),
        #         NasaSource("PM25/2012.tif",
        #                    datetime(2012, 1, 1),
        #                    datetime(2012, 12, 31)),
        #         NasaSource("PM25/2013.tif",
        #                    datetime(2013, 1, 1),
        #                    datetime(2013, 12, 31)),
        #         NasaSource("PM25/2014.tif",
        #                    datetime(2014, 1, 1),
        #                    datetime(2014, 12, 31)),
        #         NasaSource("PM25/2015.tif",
        #                    datetime(2015, 1, 1),
        #                    datetime(2015, 12, 31)),
        #         NasaSource("PM25/2016.tif",
        #                    datetime(2016, 1, 1),
        #                    datetime(2016, 12, 31))
        #     ],
        #     (SedohDataSource.NASA, 'O3'): [
        #         NasaSource("O3/2000.tif",
        #                    datetime(2000, 1, 1),
        #                    datetime(2000, 12, 31)),
        #         NasaSource("O3/2001.tif",
        #                    datetime(2001, 1, 1),
        #                    datetime(2001, 12, 31)),
        #         NasaSource("O3/2002.tif",
        #                    datetime(2002, 1, 1),
        #                    datetime(2002, 12, 31)),
        #         NasaSource("O3/2003.tif",
        #                    datetime(2003, 1, 1),
        #                    datetime(2003, 12, 31)),
        #         NasaSource("O3/2004.tif",
        #                    datetime(2004, 1, 1),
        #                    datetime(2004, 12, 31)),
        #         NasaSource("O3/2005.tif",
        #                    datetime(2005, 1, 1),
        #                    datetime(2005, 12, 31)),
        #         NasaSource("O3/2006.tif",
        #                    datetime(2006, 1, 1),
        #                    datetime(2006, 12, 31)),
        #         NasaSource("O3/2007.tif",
        #                    datetime(2007, 1, 1),
        #                    datetime(2007, 12, 31)),
        #         NasaSource("O3/2008.tif",
        #                    datetime(2008, 1, 1),
        #                    datetime(2008, 12, 31)),
        #         NasaSource("O3/2009.tif",
        #                    datetime(2009, 1, 1),
        #                    datetime(2009, 12, 31)),
        #         NasaSource("O3/2010.tif",
        #                    datetime(2010, 1, 1),
        #                    datetime(2010, 12, 31)),
        #         NasaSource("O3/2011.tif",
        #                    datetime(2011, 1, 1),
        #                    datetime(2011, 12, 31)),
        #         NasaSource("O3/2012.tif",
        #                    datetime(2012, 1, 1),
        #                    datetime(2012, 12, 31)),
        #         NasaSource("O3/2013.tif",
        #                    datetime(2013, 1, 1),
        #                    datetime(2013, 12, 31)),
        #         NasaSource("O3/2014.tif",
        #                    datetime(2014, 1, 1),
        #                    datetime(2014, 12, 31)),
        #         NasaSource("O3/2015.tif",
        #                    datetime(2015, 1, 1),
        #                    datetime(2015, 12, 31)),
        #         NasaSource("O3/2016.tif",
        #                    datetime(2016, 1, 1),
        #                    datetime(2016, 12, 31))
        #     ]
        # }


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
                        GetStrategy.PRIVATE_API,
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
