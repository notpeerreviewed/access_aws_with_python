## Data Warehouse project - Udacity Data Engineering Nano Degree

### Summary

This project involved writing a series of python scripts to provision and configure a Redshift cluster, create a series of database tables, and populate the tables with data from a set of json files which were staged in intermediate tables.

 

### Files and order of execution

This project includes the requisite python scripts as included in the template, plus an additional script to generate the Redshift cluster beforehand and a script to allow easy deletion of the Redshift cluster after use.

There is an additional script that includes several test queries to examine the data loaded into the tables.

The files should be executed in the following order:

- create_redshift.py
- create_tables.py
- etl.py
- delete_redshift.py

#### create_redshift.py

This script is a convenient way to create a new redshift cluster using the specified configuration items in the dwh.cfg file. The config file includes the following elements:

- DWH_CLUSTER_TYPE=multi-node
- DWH_NUM_NODES=4
- DWH_NODE_TYPE=dc2.large
- DWH_IAM_ROLE_NAME=dwhRole
- DWH_CLUSTER_IDENTIFIER=dwhCluster
- DWH_DB=dwh
- DWH_DB_USER=dwhuser
- DWH_DB_PASSWORD=Passw0rd
- DWH_PORT=5439

Users will need to specify their own security key and secret to run this file and create a redshift cluster. 

**Note**: If a user does not add their personal key and secret to the config file then this script will not run.


#### create_tables.py

This script defines two helper functions to drop existing tables and create the new tables using sql statements specified in the sql_queries.py script.

The main function tests for the existence of a cluster and if no cluster is available, returns a string warning the user to ensure they have run the create_cluster.py file first. 

If a cluster exists, and is available, the drop tables and create tables functions are run to create the new empty tables required for the project.

#### etl.py

This file is largely unchanged from the template file provided with the only exception being a slightly different connection function. This script will populate the staging tables and the main tables of the database from the songs data.

#### delete_redshift.py

This is convencience function to simplify deletion of the redshift cluster after the project has been run.