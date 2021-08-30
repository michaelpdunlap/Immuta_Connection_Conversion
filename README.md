# Immuta_Connection_Conversion
Utilities to convert Immuta data source connections under specific circumstances.

## connection_conversion.py
This is a python script designed to swap Immuta databricks connections from Workspace A to Workspace N.  It is meant to be run via Databricks for ease of use in the customer environment but could be ported as needed.  The script functionality is driven by a .csv file that contains a source to target mapping tailored to your environment.

## Install
1.) Download the latest .py files.  
2.) Download the accompanying .csv template.  
3.) (optional) Use the connection extractor script to gather the source connections you need to convert. 
4.) Populate the CSV template as needed to map your connection conversion.  
5.) Update the Immuta specific variables in the script.  
6.) Unleash hell.  Script actions are printed to the command line.  You can redirect that according to your requirement.  

## ProjectSourceExtractor.py

This script will extract schema project and data source connections. It will return a dataframe that you can export to your file system.

It currently relies upon entering a search string to narrow down the results returned. It's defaulted to 443, the Databricks port.

The script will provide a mapping of schema projects and their connections with the subordinate data source connections.

### Instruction
1.) Download the latest .py file
2.) Update to suit your environment, note the directory where you will drop the .csv file
3.) Execute the script

## get_instance_connections.py

Returns a distinct list of all data source connections for an Immuta.
