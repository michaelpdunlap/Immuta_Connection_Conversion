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
# adding this to troubleshoot auth errors
print(authResponse)
authToken = authResponse["token"]
# print(authToken)

# now let's try to get all connections for a project
  
projectResponse = requests.get(
  IMMUTA_URL + '/project?size=1000',
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
UnknownProjectCnt = 0
DataSourceCnt = 0
MatchCnt = 0
DontMatchCnt = 0
FixCnt = 0

for i in projectIDs:
  projectID = i['id']
  # count the data sources for this project
  projectDataSourceCnt = 0
  # of connections for this project starts at 1
  projectConnectionCnt = 1
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
      
      # remove the extra bits.  May need to check for null / from.
        
      if "from " in projectConnection:   
          projectConnection = projectConnection.split("from ",1)[1]
          projectConnection = projectConnection.rstrip(projectConnection[-1])
      
      projectDataSources = requests.get(
        IMMUTA_URL + "/project/" + str(i['id']) + "/dataSources?size=1000",
        headers={'Authorization': authToken }
      )
      
      dataSources = projectDataSources.json()
      dataSourceIDs = dataSources['dataSources']
      # get the info for each data source, and append relevant project info
      
      for z in dataSourceIDs:
        # iterate count of total data sources
        DataSourceCnt = DataSourceCnt + 1
        # iterate count of data sources for this project
        projectDataSourceCnt = projectDataSourceCnt + 1
        # from here, need to validate strongly due to inavailability of resources
        dataSourceID = z['dataSourceId']
        dataSourceName = z['dataSourceName']
        dataSourceConnection = z['connectionString']
        # ok, now let's tally if they match
        # project matches data source
        if projectConnection == dataSourceConnection:
            MatchCnt = MatchCnt + 1
            matchValue = "Y"
        # project doesn't match data source
        else: 
            # it's not the first record
            if projectDataSourceCnt > 1:
                # connection doesn't match, >1 record = problem
                if lastDataSourceConnection != dataSourceConnection:
                    #set a var to indicate >1 data source connections for schema project
                    projectConnectionCnt = projectConnectionCnt + 1
                    matchValue = "FixMe"
                    DontMatchCnt = DontMatchCnt + 1
                # connection does match the last connection
                else:
                    # but still >1 connections for data source = problem
                    if projectConnectionCnt > 1:
                        matchValue = "FixMe"
                        DontMatchCnt = DontMatchCnt + 1
                    # doesn't match project, but still only one data source : schema connection
                    else:
                        matchValue = "BrokenDescription"
                        DontMatchCnt = DontMatchCnt + 1
            # it's the first record and it doesn't match
            else:        
                DontMatchCnt = DontMatchCnt + 1
                matchValue = "N"
        

        # store the data source connection and count to compare next iteration
        
        lastDataSourceConnection = dataSourceConnection
        
        lastProjectConnectionCnt = projectConnectionCnt 
        
    
        # at this point, start appending to lists
        projectNameList.append(projectName)
        projectConnectionList.append(projectConnection)
        dataSourceIDList.append(dataSourceID)
        dataSourceNameList.append(dataSourceName)
        dataSourceConnectionList.append(dataSourceConnection)
        matchList.append(matchValue)
        
    # trap any schema projects that don't match the search string
    else:
      print ("Project ID " + str(i['id']) + " may be a schema project, but doesn't match search string.")
      # generate count of schema projects
      UnknownProjectCnt = UnknownProjectCnt + 1
      # for each source type schema project, get the connections for the underlying data sources
      # call the get project data sources end point
      
      projectName = i['name']
      projectConnection = i['description']

      # remove the extra bits.  May need to check for null / from.
        
      if "from " in projectConnection:   
          projectConnection = projectConnection.split("from ",1)[1]
          projectConnection = projectConnection.rstrip(projectConnection[-1])
      
      projectDataSources = requests.get(
        IMMUTA_URL + "/project/" + str(i['id']) + "/dataSources?size=1000",
        headers={'Authorization': authToken }
      )
      
      dataSources2 = projectDataSources.json()
      dataSourceIDs2 = dataSources2['dataSources']
      
      # print (dataSourceIDs2)
      # get the info for each data source, and append relevant project info
      
      for x in dataSourceIDs2:
        # generate count of data sources
        DataSourceCnt = DataSourceCnt + 1
        # iterate count of data sources for this project
        projectDataSourceCnt = projectDataSourceCnt + 1
        # from here, need to validate strongly due to inavailability of resources
        dataSourceID = x['dataSourceId']
        dataSourceName = x['dataSourceName']
        dataSourceConnection = x['connectionString']
        # ok, now let's tally if they match
        # print ("This is the else branch data source name " + dataSourceName)
        # project matches data source
        if projectConnection == dataSourceConnection:
            MatchCnt = MatchCnt + 1
            matchValue = "Y"
        # project doesn't match data source
        else: 
            # it's not the first record
            if projectDataSourceCnt > 1:
                # connection doesn't match, >1 record = problem
                if lastDataSourceConnection != dataSourceConnection:
                    #set a var to indicate >1 data source connections for schema project
                    projectConnectionCnt = projectConnectionCnt + 1
                    matchValue = "FixMe"
                    FixCnt = FixCnt + 1
                # connection does match the last connection
                else:
                    # but still >1 connections for data source = problem
                    if projectConnectionCnt > 1:
                        matchValue = "FixMe"
                        FixCnt = FixCnt + 1
                    # doesn't match project, but still only one data source : schema connection
                    else:
                        matchValue = "BrokenDescription"
                        DontMatchCnt = DontMatchCnt + 1
            # it's the first record and it doesn't match
            else:        
                matchValue = "N"
                DontMatchCnt = DontMatchCnt + 1
                
            
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

print("There are " + str(UnknownProjectCnt) + " possible schema project connections.")
 
print("There are " + str(DataSourceCnt) + " data sources.")

print(str(MatchCnt) + " datasources match their project connection.")

print(str(DontMatchCnt) + " datasources do not match their project connection description.")

print(str(FixCnt) + " datasources are part of a schema project with >1 data source connections.")

# print dataframe to console or export to your file server
print(dataSourceSummary)

# file export options
# export to local
# dataSourceSummary.to_csv("~/Documents/data_connection_export.csv", index=False)
# export to DBFS - must be a non Immuta cluster due to write permission constraints
# dataSourceSummary.to_csv("/dbfs/<your favorite directory>/dataSourceSummary.csv", index=False)