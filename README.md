# Immuta_Connection_Conversion
Utility to convert Immuta data source connections under specific circumstances.

## Description
This is a python script designed to swap Immuta databricks connections from Workspace A to Workspace N.  It is meant to be run via Databricks for ease of use in the customer environment but could be ported as needed.  The script functionality is driven by a .csv file that contains a source to target mapping tailored to your environment.

## Install
1.) Download the latest .py files.  
2.) Download the accompanying .csv template.  
3.) (optional) Use the connection extractor script to gather the source connections you need to convert. 
4.) Populate the CSV template as needed to map your connection conversion.  
5.) Update the Immuta specific variables in the script.  
6.) Unleash hell.  Script actions are printed to the command line.  You can redirect that according to your requirement.  
