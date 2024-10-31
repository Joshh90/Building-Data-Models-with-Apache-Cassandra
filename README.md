# Data Modeling With Apache Cassandra-project
## Description:
Sparkify, a music streaming startup, wants insights into their user listening behavior. Currently, data is stored in CSV files, making querying difficult. They need an Apache Cassandra database to enable efficient song play queries. As a data engineer, I designed a data model to support the analysis team in generating valuable insights.

### About dataset
This dataset contains information about Sparkify,a startup that want to analyze the data they've been collecting on songs and user activity on their new music streaming app


### Diagram
![Diagram](https://github.com/Joshh90/data-modeling-with-apache-cassandra-project/blob/main/Cassandra_logo.svg)

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
