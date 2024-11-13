# Data Modeling With Apache Cassandra-project
## Description:
Sparkify, a music streaming startup, wants insights into their user listening behavior. Currently, data is stored in CSV files, making querying difficult. They need an Apache Cassandra database to enable efficient song play queries. As a data engineer, I designed a data model to support the analysis team in generating valuable insights.

### Diagram
![Diagram](https://github.com/Joshh90/data-modeling-with-apache-cassandra-project/blob/main/Cassandra_logo.svg)

### About dataset
This dataset contains information about Sparkify,a startup that want to analyze the data they've been collecting on songs and user activity on their new music streaming app.
[Dataset link](https://github.com/Joshh90/data-modeling-with-apache-cassandra-project/blob/main/event_datafile_new.csv)

### Screenshot of denormalized event_datafile_new.csv dataset
![Screenshot](https://github.com/Joshh90/data-modeling-with-apache-cassandra-project/blob/main/screenshot_of_event_datafile.jpg)

### Services Used
1. **Jupiter notebook:** is an open-source web application that allows you to create and share documents that contain live code, equations, visualizations, and narrative text. It is widely used in data science, machine learning, and data analysis.

2. **Python for dataengineering:** refers to the use of the Python programming language to perform various tasks related to data engineering, which includes the design, construction, and management of systems and processes that collect, store, and analyze data. Python is a popular choice in data engineering due to its simplicity, versatility, and the extensive ecosystem of libraries and frameworks available for data manipulation, analysis, and processing.

3. **Sql:** Sql, or Structured Query Language, is a standard programming language specifically designed for managing and manipulating relational databases.


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
### Project Execution Flow
Data upload to jupyter notebook working directory --> Dataset extraction from jupyter notebook working directory --> Pre-process data with Python --> Create cassandra instance cluster --> Create keyspace in cassandra --> Set keyspace in cassandra --> Modelling the database tables based on queries -->Insert data into the database table --> Query the table to gain insight
