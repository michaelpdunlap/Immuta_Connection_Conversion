#################################
#                               #
#       Project Source          #
#          Extractor            #
#                               #
# By: Mike Dunlap, Immuta CSA   #
# Born On: 8/27/2021            # 
#                               #
#################################

# Purpose
# Provide a snapshot of schema project connections and the connections
# of the underlying data source
#
# Basic instruction
# Run the script - export the data to the file & filesystem of your choice

#Install json
import json

#Import requests
import requests

#Import Pandas
import pandas

# set Immuta vars
# Set the hostname (and port if not 443) for the Immuta instance
IMMUTA_URL= "https:yourimmuta.immuta.com"
# This is your user API key from Immuta
API_KEY= "yourapikey"

# use a source specific string to detect a type of connection
# Databricks source type
sourceTypeSearch = "443" 

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

# now let's try to get all connections for a project
  
projectResponse = requests.get(
  IMMUTA_URL + '/project',
  headers={'Authorization': authToken }  
)
  
# projectResponse.json()
  
# print (projectResponse.json())

# start lists to form pandas df

projectNameList = []
projectConnectionList = []
dataSourceIDList = []
dataSourceNameList = []
dataSourceConnectionList = []
matchList = []
  
# swell, now let's loop through the results
  
projects = projectResponse.json()
projectIDs = projects['hits']

# let's establish some base data metrics

SchemaProjectCnt = 0
DataSourceCnt = 0
MatchCnt = 0
DontMatchCnt = 0

for i in projectIDs:
  projectID = i['id']
  # projectDescription = i['description']
  # detect databricks schema projects
  if i['description'] == None:
    print ("Project ID " + str(i['id']) + " is not a schema project.")
  else:
    if sourceTypeSearch in i['description']: 
      print ("Project ID " + str(i['id']) + " is indeed a schema project.")
      # generate count of schema projects
      SchemaProjectCnt = SchemaProjectCnt + 1
      # for each source type schema project, get the connections for the underlying data sources
      # call the get project data sources end point
      
      projectName = i['name']
      projectConnection = i['description']
      # remove the extra bits
      projectConnection = projectConnection.split("from ",1)[1]
      projectConnection = projectConnection.rstrip(projectConnection[-1])
      
      projectDataSources = requests.get(
        IMMUTA_URL + "/project/" + str(i['id']) + "/dataSources",
        headers={'Authorization': authToken }
      )
      
      dataSources = projectDataSources.json()
      dataSourceIDs = dataSources['dataSources']
      # get the info for each data source, and append relevant project info
      
      for z in dataSourceIDs:
        # generate count of data sources
        DataSourceCnt = DataSourceCnt + 1
        # from here, need to validate strongly due to inavailability of resources
        dataSourceID = z['dataSourceId']
        dataSourceName = z['dataSourceName']
        dataSourceConnection = z['connectionString']
        # ok, now let's tally if they match
        if projectConnection == dataSourceConnection:
            MatchCnt = MatchCnt + 1
            matchValue = "Y"
        else:
            DontMatchCnt = DontMatchCnt + 1
            matchValue = "N"

        # gotta figure out here how to add to owners, there is a call for that
    
        # at this point, start appending to lists
        projectNameList.append(projectName)
        projectConnectionList.append(projectConnection)
        dataSourceIDList.append(dataSourceID)
        dataSourceNameList.append(dataSourceName)
        dataSourceConnectionList.append(dataSourceConnection)
        matchList.append(matchValue)

# build and print final data frame

sourceDictionary = {"ProjectName":projectNameList, "ProjectConnection":projectConnectionList, 
                   "DataSourceID":dataSourceIDList, "DataSourceName":dataSourceNameList, 
                    "DataSourceConnection":dataSourceConnectionList, "Matches":matchList}

dataSourceSummary = pandas.DataFrame(sourceDictionary)

# export logic

# print summary metrics

print("There are " + str(SchemaProjectCnt) + " DBx schema project connections.")
 
print("There are " + str(DataSourceCnt) + " DBx data source connections.")

print(str(MatchCnt) + " datasources match their project connection.")

print(str(DontMatchCnt) + " datasources do not match their project connection.")

# print dataframe to console or export to your file server
# print(dataSourceSummary)
dataSourceSummary.to_csv("~/Documents/data_connection_export.csv", index=False)