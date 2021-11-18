# Data Engineering Capstone Project
## Project Summary
The objective of this project was to create an ETL pipeline for I94 immigration, US demographics datasets and airport datasets to form an analytics database on immigration events enriched by geographic data.

A use case for this analytics database is to find immigration patterns to the US. For example, we could try to find answears to questions such as, from which country immigration has taken to which US city.

## Data and Code
The immigration data was provided by Udacity in SAS (sas7bdat) format. It contained data from 2016, one file per month. All together this dataset contained around 40 million entries.
The demographic data was given as a csv file containing data about around 3000 cities in the US. The data contains information about the population of each city, location of the city (US state) and race of the inhabitants.
The airport data was provided as csv file containing geographic and metadata regarding airports including the IATA code.

### In addition to the data files, the project workspace includes:

etl.py - reads data, processes that data using Spark, and create temporary tables
quality_check_functions.py and CodeUtilities.py - these modules contains the functions for quality checks and for transformation of given data into US state code and city code.

country_codes.txt,us_state_code.txt, city_codes.txt - extracted from I94_SAS_Labels_Description.SAS (provided by Udacity) containing mappping information for countries, states and cities

Jupyter Notebooks - jupyter notebook that was used for building the ETL pipeline.

### Prerequisites

Apache Spark


### The project follows the following steps:
Step 1: Scope the Project and Gather Data
Step 2: Explore and Assess the Data
Step 3: Define the Data Model
Step 4: Run ETL to Model the Data
Step 5: Complete Project Write Up

#### Step 1: Scope the Project and Gather Data
Project Scope
To create the analytics database, the following steps will be carried out:

Use Spark to load the data into dataframes.
Exploratory data analysis of I94 immigration dataset to identify missing values and strategies for data cleaning.
Exploratory data analysis of demographics dataset to identify missing values and strategies for data cleaning.
Exploratory data analysis of airport dataset.


#### Step 2: Explore and Assess the Data
Refer to the jupyter notebook for exploratory data analysis

#### Step 3: Define the Data Model
3.1 Conceptual Data Model

The immigration data column i94port is used to correlate immigration data with demographic data and airport data. Therefor in the demographic dataset columns City and "State Code" where used to derive a city code matching to the city
code of the immigration data.
For the airports the municipality and state_code data is used to map to a city code of the immigration data.


3.2 Mapping Out Data Pipelines
The pipeline steps are as follows:

Load the datasets
Clean the I94 Immigration data to create Spark dataframe for each month
Create visa_type dimension table
Create immigration fact table
Load demographics data
Clean demographics data
Create demographic dimension table
#### Step 4: Run Pipelines to Model the Data
4.1 Create the data model
Refere to the jupyter notebook for the data dictionary.

4.2 Running the ETL pipeline
The ETL pipeline is defined in the etl.py script, and this script uses the CodeUtilities.py and quality_functions.py modules to create a pipeline that creates final tables.


