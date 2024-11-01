#!/usr/bin/env python
# coding: utf-8

# # Part I. ETL Pipeline for Pre-Processing the Files

# #### Import Python packages 

# In[92]:


# Import Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv


# #### Creating list of filepaths to process original event csv data files

# In[93]:


print(os.getcwd())  # checking the current working directory


filepath = os.getcwd() + '/event_data' # Getting the current folder and subfolder event data


for root, dirs, files in os.walk(filepath): #Using a for-loop to create a list of files and collect each filepath
    

    file_path_list = glob.glob(os.path.join(root,'*')) #joining the file path and roots with the subdirectories using glob


# #### Processing the files to create the data file csv that will be used for Apache Casssandra tables

# In[94]:


# initiating an empty list of rows that will be generated from each file
full_data_rows_list = []
    
# for every filepath in the file path list
for f in file_path_list:

# reading csv file 
    with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
        # creating a csv reader object 
        csvreader = csv.reader(csvfile) 
        next(csvreader)
        
 # extracting each data row one by one and append it        
        for line in csvreader:
            #print(line)
            full_data_rows_list.append(line) 
            
# Getting the total number of rows 
#print(len(full_data_rows_list))
# Checking to see what the list of event data rows will look like
print(full_data_rows_list)

# creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
# Apache Cassandra tables
csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist','firstName','gender','itemInSession','lastName','length','level','location','sessionId','song','userId'])
    for row in full_data_rows_list:
        if (row[0] == ''):
            continue
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))


# In[95]:


# checking the number of rows in the csv file
with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    print(sum(1 for line in f))


# # Part II. Apache Cassandra coding portion of the project. 
# 
# ## Working with the CSV file titled <font color=red>event_datafile_new.csv</font>, located within the Workspace directory. 
# 
# The image below is a screenshot of what the denormalized data look like in the <font color=red>**event_datafile_new.csv**</font> after the code above is run:<br>
# 
# <img src="images/image_event_datafile_new.jpg">

# #### Creating a Cluster

# In[97]:


# This should make a connection to a Cassandra instance in the local machine 
# (127.0.0.1)

from cassandra.cluster import Cluster
cluster = Cluster()

# To establish connection and begin executing queries, need a session
session = cluster.connect()


# #### Create Keyspace

# In[101]:


# TO-DO: Create a Keyspace
try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS new_sparkify
    WITH REPLICATION =
    {'class': 'SimpleStrategy', 'replication_factor':1}"""
)
except EXCEPTION as e:
    print(e)


# #### Set Keyspace

# In[102]:


# TO-DO: Set KEYSPACE to the keyspace specified above
try:
    session.set_keyspace('new_sparkify')
except EXCEPTION as e:
    print(e)


# ### Next Step: I created tables to run the following queries:  
# #### N.B: I modelled the database tables on the queries I want to run.

# ## Query 1

# ### Goal: Retrieve the artist, song title, and length of a song that was heard during a specific sessionId and itemInSession

# ### Table name: 'artist_songtitle_and_length_table'

# #### Table design:

# In[103]:


query = """
CREATE TABLE IF NOT EXISTS artist_songtitle_and_length_table(
    artist text, 
    song text, 
    sessionId int, 
    itemInSession int, 
    length double, 
    PRIMARY KEY ((sessionId), itemInSession)
)
"""
try:
    session.execute(query)
except Exception as e:
    print(f"Error executing query: {e}")


# ### Assign the INSERT statement to the query variable
# 

# In[104]:


# Assign the INSERT statement to the query variable
file = 'event_datafile_new.csv'

with open(file, encoding='utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader)  # Skip the header row
    
    for line in csvreader:
        
        query = """
        INSERT INTO artist_songtitle_and_length_table (artist, song, sessionId, itemInSession, length)
        VALUES (%s, %s, %s, %s, %s)
        """
        try:
            # Execute the query with the correct column mappings and type casting
            session.execute(query, (
                line[0],           # artist
                line[9],           # song
                int(line[8]),      # sessionId
                int(line[3]),      # itemInSession
                float(line[5])     # length
            ))
        except Exception as e:
            print(f"Error inserting row {line}: {e}")
    
        ## For e.g., to INSERT artist_name and user first_name, you would change the code below to `line[0], line[1]`
        ##session.execute(query, (line[#], line[#]))


# In[105]:


query = """
SELECT artist, song, length 
FROM artist_songtitle_and_length_table 
WHERE sessionId = 338 AND itemInSession = 4;
"""

try:
    # Execute the query
    rows = session.execute(query)

    # Print the results
    for row in rows:
        print(row.artist, row.song, row.length)
except Exception as e:
    print(f"Error executing query: {e}")


# #### Explanation: Partition Key: sessionId – Groups rows by session, as the query specifically looks for a particular sessionId.
# #### Clustering Key: itemInSession – Orders records within each session, allowing us to efficiently retrieve data for a specific song in the session.
# #### This design enables Cassandra to retrieve data by sessionId and itemInSession efficiently, which aligns with the query requirements.

# ## Query 2

# ### Goal: Retrieve the artist name, song title (sorted by itemInSession), and user information (first and last name) for a specific userId and sessionId.

# #### Table name: 'artist_sortedsong_and_users_table'

# #### Table design

# In[106]:


query= "CREATE TABLE IF NOT EXISTS artist_sortedsong_and_users_table"
query= query + "(userId int,sessionId int,itemInSession int,artist text,song text,firstname text,lastname text,PRIMARY KEY ((userId, sessionId), itemInSession))"
try:
    session.execute(query)
except Exception as e:
    print(f"Error inserting line {line}: {e}")                 


# ### Assign the INSERT statement to the query variable
# 

# In[107]:


# Assigning the INSERT statement into the `query` variable
file = 'event_datafile_new.csv'

with open(file, encoding='utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader)  # Skip the header

    for line in csvreader:
        query = """
        INSERT INTO artist_sortedsong_and_users_table (userId,sessionId,itemInSession,artist, song, firstname, lastname)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        # Assigning column values from the CSV line for the INSERT statement
        try:
            session.execute(
                query,
                (int(line[10]), # userId (correct index, was line[9] before)
                 int(line[8]) , # sessionId (should be int)
                 int(line[3]) , # itemInSession (should be int)
                    line[0],    # artist
                    line[9],    # song
                    line[1],    # firstName
                    line[4]     # lastName
                )
            )
        except Exception as e:
            print(f"Error inserting line {line}: {e}")


# In[108]:


query = """
SELECT artist,song,firstname,lastname 
FROM artist_sortedsong_and_users_table 
WHERE userId = 10 AND sessionId = 182
ORDER BY itemInSession
"""
try:
    rows = session.execute(query)
except Exception as e:
    print(f"Error executing query: {e}")
for row in rows:
    print(row.artist, row.song,row.firstname, row.lastname)


# #### Explanation:
# #### Partition Key: (userId, sessionId) – This composite partition key allows us to group data by both userId and sessionId, which is necessary for efficiently retrieving songs a user listened to in a specific session.
# #### Clustering Key: itemInSession – This ensures the songs are sorted by itemInSession within each userId and sessionId group, which aligns with the query requirement to retrieve songs in the order they were listened to.

# ## Query 3: 

# ## Goal: Retrieve every user's first and last name who listened to a specific song, in this case, "All Hands Against His Own".

# #### Table name : 'AllHandsAgainstHisOwn_listeners_table'

# #### Table design:

# In[109]:


query = """
CREATE TABLE IF NOT EXISTS AllHandsAgainstHisOwn_listeners_table (
    userId int,
    firstname text,
    lastname text,
    song text,
    PRIMARY KEY (song, userId)
);
"""

try:
    session.execute(query)
except Exception as e:
    print(f"Error creating table: {e}")


# ### Assign the INSERT statement to the query variable

# In[110]:


file = 'event_datafile_new.csv'
with open(file, encoding='utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader)  # Skipping the header row

    for line in csvreader:
        # Constructing the INSERT statement
        query = """
        INSERT INTO AllHandsAgainstHisOwn_listeners_table (userId, sessionId, itemInSession, firstname, lastname, song)
        VALUES (%s, %s, %s, %s,%s,%s);
        """
        
        try:
            # Executing the INSERT statement with values from the line
            session.execute(query, (
                int(line[10]),      # userId
                int(line[8]),      # sessionId
                int(line[3]),      # itemInSession
                line[1],           # firstName
                line[4],           # lastName
                line[9]            # song
            ))
        except Exception as e:
            print(f"Error inserting row {line}: {e}")


# In[111]:


query = """
SELECT firstname, lastname 
FROM  AllHandsAgainstHisOwn_listeners_table
WHERE song = 'All Hands Against His Own';
"""

try:
    rows = session.execute(query)
    for row in rows:
        print(row.firstname, row.lastname)
except Exception as e:
    print(f"Error executing query: {e}")


# #### Explanation:
# #### Partition Key: song and userId – Allows us to efficiently query by song by song title and userId, which is the main filter criterion.
# #### Composite primary key: song, userId – Ensure uniqueness and ordering within each song partition, allowing retrieval of users who listened to a particular song.
# #### This design is optimized for querying by song title and userId retrieving all users associated with a specific song.

# ### Droping the tables before closing out the sessions

# In[112]:


query = "drop table artist_songtitle_and_length_table"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)


# In[113]:


query = "drop table artist_sortedsong_and_users_table"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)


# In[114]:


query = "drop table AllHandsAgainstHisOwn_listeners_table"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)


# ### Close the session and cluster connection¶

# In[115]:


session.shutdown()
cluster.shutdown()

