# CDR-analysis-tools-hadoop

Like the standalone version, this repository is a set of tools written in Python for analyzing Call Detail Records (CDRs) data, additionally based on the hadoop platform which supports a large amount of data. The analysis includes Visualization (with reports and processed data compatible with other visualization platforms), Origin-Destination (OD) and Interpolation. 

This repository will be incrementally updated from time to time. Kindly visit the repository and have a look at this file. 

# Data preparation

The user needs 2 files for the tool, a cdr file and a location mapping file. Both of them come with different column names and formats. To process CDR data, the data needs to be in the format that is compatible with the tools. The mapping json file maps from your prepared raw csv files to Hive tables ready for the processing and a mapping scheme for each file has to be done by the user. 

## a CSV file for CDR records
To analyse the CDR data, the user needs to provide the tools with a CDR file in the csv format. It needs to contain
1. IMEI or IMSEI 
2. Call start time
3. Cell ID
4. Call Type

### Mapping

## a CSV location mapping file for administration units
The previous csv file will be joined with this cell id file to calculate zone-based statistics. It should supply
1. Cell ID (will be joined with the Cell ID in the CDR record file)
2. At least one Administration Unit (ex. province or district) name
3. Latitude
4. Longitude

### Mapping



# Prerequisites
  * Hadoop server with Hive installed
  * Python 3 or above 
  * Python pip3 (a Python package installer)
  
# Installation
 * install all requirement packages in requirements.txt 
  * pip install -r requirements.txt
