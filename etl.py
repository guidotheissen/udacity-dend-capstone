import pandas as pd
import glob
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from pyspark.sql.types import StringType, IntegerType, DoubleType, BooleanType, DateType
from pyspark.sql.functions import concat, col, lit, udf
import re
import datetime as dt
from quality_check_functions import performQualityChecks,check_existence, check_not_empty, check_all_lines_loaded
from CodeUtilities import CodeUtilities

def create_spark_session():
    spark = SparkSession.builder. \
        config("spark.jars.repositories", "https://repos.spark-packages.org/"). \
        config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11"). \
        enableHiveSupport().getOrCreate()
    return spark


def process_city_data(spark, file_name):
    """
    Transform txt file into dimensional dataframe for cities
    :param spark: spark session to use
    :param file_name: txt file containing a list of cities
    :return: spark dataframe containing US cities with name and state code
    """
    city_codes = file_name
    with open(city_codes) as f:
        lines = f.readlines()
    lines = [line.replace("   '", '').replace("'", '').replace("\n", "").replace("\t", "") for line in lines]

    regexp = re.compile(r'^(.{3})=(.*),(\s*\w{2})(\s+.*|$).*')
    matches = [regexp.match(line) for line in lines]
    city_dict = {
        'code': list(),
        'name': list(),
        'state_code': list()
    }
    for m in matches:
        if m is None:
            continue
        city_dict['code'].append(m.group(1).strip())
        city_dict['name'].append(m.group(2).strip())
        city_dict['state_code'].append(m.group(3).strip())

    us_cities_pd = pd.DataFrame.from_dict(city_dict)
    us_cities_df = spark.createDataFrame(us_cities_pd)
    performQualityChecks(us_cities_df, city_codes, False)
    return us_cities_df


def process_state_codes(spark, file_name):
    us_state_codes = file_name
    with open(us_state_codes) as f:
        lines = f.readlines()
    lines = [line.replace("'", '').replace("\n", "").replace("\t", "") for line in lines]

    regexp = re.compile(r'^(.{2})=(.*)')
    matches = [regexp.match(line) for line in lines]
    us_state_dict = {
        'code': list(),
        'name': list(),
    }
    for m in matches:
        if m is None:
            continue
        us_state_dict['code'].append(m.group(1))
        us_state_dict['name'].append(m.group(2))

    us_states_pd = pd.DataFrame.from_dict(us_state_dict)
    us_states_df = spark.createDataFrame(us_states_pd)
    check_not_empty(us_states_df, us_state_codes)
    check_all_lines_loaded(us_states_df, us_state_codes, False)
    return us_states_df

def process_country_codes(spark, file_name):
    """
    Transform txt file into dimensional dataframe for countries
    :param spark: spark session to use
    :param file_name: txt file containing a list of cities
    :return: spark dataframe containing countrie with name and ISO code
    """
    country_codes = file_name
    with open(country_codes) as f:
        lines = f.readlines()
    lines = [line.replace('"', '').replace('\n', '').replace("'", '') for line in lines]

    regexp = re.compile(r"^\s*(\d{3})\s*=\s*(.*).*")
    matches = [regexp.match(line) for line in lines]
    country_dict = {
        'country_code': list(),
        'country_name': list()
    }

    for m in matches:
        if m is None:
            continue
        country_dict['country_code'].append(int(m.group(1)))
        country_dict['country_name'].append(m.group(2))

    countries_pd = pd.DataFrame.from_dict(country_dict)
    countries_df = spark.createDataFrame(countries_pd)
    check_not_empty(countries_df, country_codes)
    check_all_lines_loaded(countries_df, country_codes, False)
    return countries_df

def process_airport_data(spark, file_name):
    """
    process airport data from csv file
    :param spark: spark session to use
    :param file_name: file to be processed
    :param us_cities_df: spark dataframe containing US Cities
    :return:
    """
    airports_df = spark.read.csv(file_name, inferSchema=True, header=True, sep=',')
    check_not_empty(airports_df, file_name)
    check_all_lines_loaded(airports_df, file_name, True)

    airports_df = airports_df.filter("iso_country='US'").filter("iata_code != 'none'")
    getStateCodeUDF = udf(lambda iso_region: CodeUtilities.getStateCode(iso_region), StringType())
    airports_df = airports_df.withColumn("state_code", getStateCodeUDF(sf.col("iso_region")))

    us_cities_df = process_city_data(spark, 'city_codes.txt')

    cu = CodeUtilities(us_cities_df)

    getCityCodeUDF = udf(lambda c, sc: cu.getCityCode(c, sc), StringType())

    airports_df = airports_df.withColumn("city_code", getCityCodeUDF(sf.col("municipality"), sf.col("state_code")))
    airports_df.filter("city_code != '000'").count()
    return airports_df


def etl_demographics_data(spark, file_name):
    # extract
    demographics_df = spark.read.csv(file_name, inferSchema=True, header=True, sep=';')
    #transform
    demographics_df = demographics_df.withColumnRenamed('State Code', 'state_code')

    performQualityChecks(demographics_df, file_name, True)
    check_existence(demographics_df, '((City=\'Silver Spring\') AND (state_code=\'MD\'))')

    us_cities_df = process_city_data(spark, 'city_codes.txt')

    getCityCodeUDF = udf(lambda c, sc: getCityCode(us_cities_df, c, sc), StringType())

    demographics_df = demographics_df.withColumn("Median Age", demographics_df["Median Age"].cast(DoubleType())) \
        .withColumn("Male Population", demographics_df["Male Population"].cast(IntegerType())) \
        .withColumn("Female Population", demographics_df["Female Population"].cast(IntegerType())) \
        .withColumn("Total Population", demographics_df["Total Population"].cast(IntegerType())) \
        .withColumn("Count", demographics_df["Count"].cast(IntegerType())) \
        .withColumn("city_code", getCityCodeUDF(sf.col("City"), sf.col("State Code"))) \
        .withColumnRenamed('total population', 'total_population')

    demographics_df = demographics_df.select("City", "State", "Male Population", "Female Population",
                                             "Total Population", "State Code", "Race", "Count")

    demographics_df = demographics_df.filter("city_code != '000'")
    # load
    demographics_table = demographics_df.createOrReplaceTempView('demographics_table')

    return demographics_table



def etl_immigrations_data(spark, source):
    # extract from staging
    immigrations_df = spark.read.parquet(source)
    # transfrom
    get_datetime = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(x)).isoformat() if x else None)
    # Convert i94cit and i94res to integer and replace them with country codes
    immigrations_df = immigrations_df.withColumn("arrdate", get_datetime(immigrations_df.arrdate))\
                                     .withColumn("i94cit", col("i94cit").cast(IntegerType())) \
                                     .withColumn("i94res", col("i94res").cast(IntegerType()))
    # load
    immigrations_df.createOrReplaceTempView('immigrations_table')
    immigrations_table = spark.sql("""
        SELECT 
            i94yr as year,
            i94mon as month,
            i94cit as origin_city,
            i94port as destination_city,
            arrdate as arrival_date,
            visatype as visatype,
            (CASE WHEN i94visa = '1.0' 
                THEN 'business'
                ELSE 
                    (CASE WHEN i94visa = '3.0' 
                         THEN 'student' 
                         ELSE (CASE WHEN i94visa = '2.0' 
                                  THEN 'pleasure' 
                                  ELSE 'unknown'
                              END)
                    END)
            END) as visa
        FROM immigrations_table
    """)
    return immigrations_table
