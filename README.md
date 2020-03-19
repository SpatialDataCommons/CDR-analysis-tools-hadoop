# CDR-analysis-tools-hadoop

Like the standalone version, this repository is a set of tools written in Python for analyzing Call Detail Records (CDRs) data, additionally based on the hadoop platform which supports a large amount of data. The analysis includes Visualization (with reports and processed data compatible with other visualization platforms), Origin-Destination (OD) and Interpolation. 

This repository will be incrementally updated from time to time. Kindly visit the repository and have a look at this file. 

# Data preparation

# Mapping Data
To process CDR data, raw data needs to be in the format that is compatible with the tools. The mapping data maps from your prepared raw csv files to Hive tables ready for the processing.

# Prerequisites
  * Hadoop server with Hive installed
  * Python 3 or above 
  * Python pip3 (a Python package installer)
  
# Installation
 * install all requirement packages in requirements.txt 
  * pip install -r requirements.txt
