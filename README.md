# Clustering
## Prerequisites
You need to have running PostgreSQL database at your PC and configure it in `/src/main/resources/db/db.properties`
## Description
This is an implementation of an algorithm, described in <a href="https://ieeexplore.ieee.org/document/1266134">article</a><br>
The main purpose of that program is to cluster search engine logs using information about user clicks. Sample data could be found <a href="http://www.cim.mcgill.ca/~dudek/206/Logs/AOL-user-ct-collection">here</a><br>
It consists of four parts:
- __Store__. This part is responsible for interaction with database. It uses stored procedures, located at `resources/db/*.sql`
- __Preprocessor__. This part is used for normalization of data. It includes stemming and stop words removal
- __Clusterizator__. There is a control code for clusters creation
- __Main__. The main class. It combines all other classes

The main algorithm logic locates in SQL scripts.
