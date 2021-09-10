#################################
#                               #
#       Convertosaurus          #
#                               #
# By: Mike Dunlap, Immuta CSA   #
# Born On: 7/28/2021            # 
#                               #
#################################

# Purpose
# Based on a parameter file, convert databricks connections from workspace A to workspace B
# Basic instruction

#Install json
import json

#Import requests
import requests

#Import Pandas
import pandas

#import time
import time
import datetime

# Immuta Instance variables
# Set the hostname (and port if not 443) for the Immuta instance
IMMUTA_URL= "<your immuta here>"
# This is your user API key from Immuta
API_KEY= "<your immuta API here"
# sleep setting in seconds
SNOOZE = 10


# File location and type for forward conversion
file_location = "<your forward conversion dbfs location>"
# File location and type for backout conversion
# file_location = "<your backward conversion dbfs location"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
connections_df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

# use pandas df
pandas_df = connections_df.toPandas()

#the big loop: Pull connection string mapping

for row in pandas_df.itertuples():
  SOURCE_CONNECTION = row.SOURCE_CONNECTION
  TARGET_CONNECTION = row.TARGET_CONNECTION
  TARGET_HOST = row.TARGET_HOST
  TARGET_DB = row.TARGET_DB
  TARGET_HTTP = row.TARGET_HTTP
  TARGET_APIKEY = row.TARGET_APIKEY


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
  print(authToken)

  # now let's try to get all connections for a project
  
  projectResponse = requests.get(
    IMMUTA_URL + '/project',
    headers={'Authorization': authToken }  
  )
  
  projectResponse.json()
  
  # print (projectResponse.json())
  
  # swell, now let's loop through the results
  
  projects = projectResponse.json()
  projectIDs = projects['hits']
  
  # goal here would be to grep the connection string into the description
  for i in projectIDs:
    projectDescription = i['description']
    projectID = i['id']
  
    # now, if you find the connection in the project description, update the description and documentation
    if i['description'] == None:
      print (str(i['id']) + " not a schema project.")
    else:
      if SOURCE_CONNECTION in i['description']:
        #in here, call the project put and update the description and documentation
        updateProjDesc = requests.put(
          IMMUTA_URL + '/project/' + str(i['id']),
          headers={'Authorization': authToken,
                 'Content-Type': 'application/json'},
                  json={
                       "description": "This project contains all data sources under the schema, " + TARGET_DB + ", from " + TARGET_CONNECTION + ".",
                       "documentation": "This is an automatically generated project that collects data sources under the schema, " + TARGET_DB + ", from " + TARGET_CONNECTION + ". When data sources in this schema are added to the system, they will automatically be added to this project."
                       }
          )
        print ("Achieved status " + str(updateProjDesc.status_code) + " updating project ID" + str(i['id']) + "." )
      else:
        print (str(i['id']) + " not a match, bro.")
  
  # now let's fashion the put request to update the data source connection
  print ("Entering Data source update")
  # first we get the data sources matching the connection string
  dataSourceFetch = requests.get(
          IMMUTA_URL + '/dataSource?connectionString=' + SOURCE_CONNECTION,
          headers={'Authorization': authToken }  
        )
  # uncommenting here will pull payload from the connection call above
  print (dataSourceFetch.json())
  
  # swell, now let's loop through the results
  # see, there's a pattern
  
  dataSources = dataSourceFetch.json()
  dataSourceFetchIDs = dataSources['hits']
  
  for i in dataSourceFetchIDs:
    dataSourceID = i['id']
    # record job timestamp
    jobtime = datetime.datetime.now()
    # now let's make another call to update the datasource
    updateDataSource = requests.put(
      IMMUTA_URL + '/databricks/bulk',
      headers={'Authorization': authToken,
                 'Content-Type': 'application/json' },
      json={
        "ids": [i['id']],
          "handler": {"metadata":{
            "ssl":"true",
            "port":443,
            "database":TARGET_DB,
            "hostname":TARGET_HOST,
            "authenticationMethod": "Access Token",
            "httpPath":TARGET_HTTP,
            "userFiles":[],
            "password":TARGET_APIKEY,
            "connectionStringOptions": ""
          }
        }
      }
    )
    print ("Achieved status " + str(updateDataSource.status_code) + " updating data source ID " + str(i['id']) + " from " + SOURCE_CONNECTION + " to " + TARGET_CONNECTION + ".")
    print ("Source Connection = " + SOURCE_CONNECTION)
    print ("Target update details: Target DB = " + TARGET_DB + " Target Host = " + TARGET_HOST + "Target HTTP = " + TARGET_HTTP)
    # this is the throttle for data source updates
    time.sleep(SNOOZE)
    

  