#################################
#                               #
#      ConnectionExtractor      #
#                               #
# By: Mike Dunlap, Immuta CSA   #
# Born On: 8/2/2021             # 
#                               #
#################################

# Purpose
# Based on your Immuta instance, extract all unique connection strings and output to a single column dataframe
# Basic instruction
# Insert the Immuta URL & API key
# you can modify the print dataframe command to create the file of your choosing



# Immuta Instance variables
# Set the hostname (and port if not 443) for the Immuta instance
IMMUTA_URL= "<your URL here"
# This is your user API key from Immuta
API_KEY= "<your API key here"

# import libraries
import requests
import pandas

# get your authentication token
response = requests.post(
  IMMUTA_URL + '/bim/apikey/authenticate',
  headers={'Content-Type': 'application/json'},
  json={
    "apikey": API_KEY
  }
)

# get the auth token out of the json response
authResponse = response.json()
authToken = authResponse["token"]
# print(authToken)

# pull all source connections

sourceResponse = requests.get(
  IMMUTA_URL + '/dataSource',
  headers={'Authorization': authToken }  
)
  
sourceResponse.json()
# print (sourceResponse.json())
  
# swell, now let's loop through the results
  
sources = sourceResponse.json()
sourceIDs = sources['hits']

# create a list for the sources - we'll need to dedup
dataSource = []
  
# goal here would be to grep the connection string into the description
for i in sourceIDs:
  sourceConnection = i['connectionString']
  dataSource.append(sourceConnection)


# let's remove the dups the list

dataSourceUnique = []

for i in dataSource:
  if i not in dataSourceUnique:
    dataSourceUnique.append(i)

# convert to dataframe for easy paste into csv

dataSourceColumn = pandas.DataFrame({'SourceConnections':dataSourceUnique})

#this is a single column dataframe
#export to your file system
print(dataSourceColumn)


