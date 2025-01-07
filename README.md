# Building Data Models with Apache Cassandra
## Description:
Mobax, a music streaming startup, wants insights into their user listening behavior. Currently, data is stored in CSV files, making querying difficult. They need an Apache Cassandra database to enable efficient song play queries. As a data engineer, I designed a data model to support the analysis team in generating valuable insights.

### Diagram
![Diagram](https://github.com/Joshh90/data-modeling-with-apache-cassandra-project/blob/main/Cassandra_logo.svg)

### About dataset
This dataset contains information about Mobax,a startup that want to analyze the data they've been collecting on songs and user activity on their new music streaming app.
[Dataset link](https://github.com/Joshh90/data-modeling-with-apache-cassandra-project/blob/main/event_datafile_new.csv)

### Screenshot of denormalized event_datafile_new.csv dataset
![Screenshot](https://github.com/Joshh90/data-modeling-with-apache-cassandra-project/blob/main/screenshot_of_event_datafile.jpg)

### Services Used
1. **Jupiter notebook:** is an open-source web application that allows you to create and share documents that contain live code, equations, visualizations, and narrative text. It is widely used in data science, machine learning, and data analysis.

2. **Python for dataengineering:** refers to the use of the Python programming language to perform various tasks related to data engineering, which includes the design, construction, and management of systems and processes that collect, store, and analyze data. Python is a popular choice in data engineering due to its simplicity, versatility, and the extensive ecosystem of libraries and frameworks available for data manipulation, analysis, and processing.

3. **Sql:** Sql, or Structured Query Language, is a standard programming language specifically designed for managing and manipulating relational databases.

**Apache Cassandra:**

is a distributed NoSQL database designed to handle large amounts of data across many commodity servers, providing high availability with no single point of failure. It is widely used for applications that require real-time, high-speed read and write operations on massive datasets.

**Key Features of Cassandra:**

-**Distributed Architecture:** Data is distributed across multiple nodes in a cluster, ensuring scalability and fault tolerance.

-**Decentralized Design:** No master node; all nodes are equal, enabling high availability and eliminating single points of failure.

-**Replication:** Data is replicated across multiple nodes (and even data centers), ensuring durability and availability.

-**Tunable Consistency:** Users can choose between strong consistency and eventual consistency depending on the application's needs.

-**High Performance:** Optimized for high write and read throughput.

-**Schema-less:** Supports dynamic schema changes, making it flexible for evolving data models.


### Install Packages
 ```
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv
 ```


 

#### N.B: I modelled the database tables on the queries I want to run. Which is an advantage of Cassandra db(NoSQL db) over relational database. Modeling the db based on the query that you want to run give advantage of faster query performance.

#### Queries used to model the database:
 ``` 1: Retrieve the artist, song title, and length of a song that was heard during a specific sessionId and itemInSession.  ```

 ``` 2: Retrieve the artist name, song title (sorted by itemInSession), and user information (first and last name) for a specific userId and sessionId. ```

 ``` 3: Retrieve every user's first and last name who listened to a specific song, in this case, "All Hands Against His Own". ```

 #### Explanation of some key portion of the code:
 **Partition Key:** (userId, sessionId) – This composite partition key allows us to group data by both userId and sessionId, which is necessary for efficiently retrieving songs a user listened to in a specific session.
 
**Clustering Key:** itemInSession – This ensures the songs are sorted by itemInSession within each userId and sessionId group, which aligns with the query requirement to retrieve songs in the order they were listened to.

### Project Execution Flow
Data upload to jupyter notebook working directory --> Dataset extraction from jupyter notebook working directory --> Pre-process data with Python --> Create cassandra instance cluster --> Create keyspace in cassandra --> Set keyspace in cassandra --> Modelling the database tables based on queries -->Insert data into the database table --> Query the table to gain insight.


