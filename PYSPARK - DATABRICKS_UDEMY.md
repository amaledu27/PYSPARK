#### Hadoop archetecture  
There are structured, semi structured {JSON, XML} and unstructured{PDF, WORD} data,which is accumulating in huge volume and we need to process it in high velocity.This is called the BIG DATA PROBLEM{3 Vs of big data - Variety, Volume, Velocity}.  
Attempts were made to solve this problems were done in two approches - Monolithic and distributed approches. Scalability, fault tolerance{HA} and cost effectiveness made distributed approch a better choice. Hadoop was one one of the first distributed system developed. it mainly had 3 parts
1. YARN - Cluster operating system.  
2. HDFS - Dsitributed Storage.  
3. Map reduce - distributed computing for data processing.  
YARN - Yet Another Resource Manager is the hadoop operating system{lets multiple programs to run on same memory} known as the hadoop cluster resouce manager, which has three main components.  
RM - Resource Manager.  
NM - Node Manager.
AM - Application Master
Hadoop cluster is a MASTER - SLAVE archetecture.  
To start, we need to install hadoop on on participating computers in the cluster {installation process is simple during which one will be selected as the master and the remaining will be selected as the worker node}. Installation process will also install a YARN resource manager {RM} in the master and a NODE MANAGER SERVICE {NM} on the worker nodes, which will regularly sends Node status reports to the RM. To run a {Data processing} app in hadoop, you need to submit it to the YRAN resource manager. Now the RM will need to run this on the cluster. RM will ask one  of the node managers to start a resource container to run an APPLICATION MASTER {AM} in the container. A container is a set of resources containing memory and CPU. 
Wen we submit another program t run in YARN, the RM will follow the same step and the application master will be running on a NODE MANAGER. ie each application will run on a diffent APPLICATION MANAGE Container on the same cluster.  
HDFS - Hadoop Distributed File system , which lets you save and retrieve data in the hadoop cluster.it has two main components  
NN - Name Node.  
DN - Data Node.  
Hadoop will install the NAME NODE server in the master. and each worker node will run a DATA NODE service. When you send a file copy command, it goes to the NAME NODE, which will redirect it to some NAME nodes. The file copy command will split the main file to  small parts{blocks} and will copy it to the data nodes. The NAME node facilitats this process and stores METADATA {File name, Directory location, File size, File blocks, block id, block sequence, block location}. Now, when  there is a read operation request, the NAME NODE will all the required information to reassemble the file. The read API will receive data blocks from the DATA NODES, and reassebles the file.  
MAP reduce is a programming model{a technique or way of solving problems} and programming framework. MAP reduce framework is a set of APIs and services that allows you to apply the MAP reduce programming model. Note that the MAP reduce programming Framework is outdated and is not used. But the programming model is still relavent.   
When there is huge file in terra or petabyte size, it creates two problems storage and processing times. Hadoop can solve bothe probelms. you know how storage works, and for processing problem, it uses the map reduce loci where it will indiviudally make the calculations for each block  in parallel and then will aggregate individual results to give the final answer this is called reduce function and will ahppen on one of th participating nodes. In essence, Map reduce will split your process in to two parts, map part will include the Parellel calculations and the reduce part where you do the consolidations. This is managed by hive engine in the background. Now instead of the map reduce, we use hive sql, spark sql etc, which can be considered an abstraction over it. Note that YARN will manage the resource allocations and HDFS manages data blocks.
![image](https://github.com/user-attachments/assets/24af3c1e-c22e-46ba-808f-1745b1d34b9e)
Hive allowed to create DB, tables and views and to RUN SQL queries on hadoop and let the programmers to stay away from writing complex map reduce processes in java.
But it also had some problems.
- it was slower than the RDBMS DB.
- development was difficult
- dependancy one single language development
- storage { in HDFS, we have to add computers to add storage, with cloud offerings, devs wanted to add easy storage}
- Resource management { for resource management, YARN ws using heavy containers, devs wanted to use lightweight ones like K8S}
SPARK Emerged with solutions to thse problems.  
![image](https://github.com/user-attachments/assets/c57207c4-2f9b-4bff-9b44-74da83a6d41e)  
Now it has become and independant set up. So now, spark runs in two setups,
1, with hadoop {Data Lake}
2, without Hadoop {Lakehouse, on cloud}
#### Data lake
Data lake brought in structured and semi structured data, did ETL using hadoop / spark and then stored it to provide data in desired format for BI and DS/ML.  
In the initial stages, Data lake had two drawbacks, it laked Transactions - consistency, and reporting performance.  
So, to solve this we started including data werehouses in data lakes but storing the processed data in data werehouses. This also eveolved over time.  

The notion of the data lake recommends that you bring data into the lake in a raw format. That means you should ingest the data into the data lake and preserve an unmodified immutable copy of the data.The ingest block of the data lake is all about identifying, implementing, and managing the right tools to bring data from the source systems to the data lake. We do not have one ingestion tool that solves the purpose in all use cases. And hence, many vendors are competing for a place in this box.  
Detailed view of data lake is below.  
<img width="710" height="407" alt="image" src="https://github.com/user-attachments/assets/7982c973-d1ba-465a-aa95-d2da5d56c929" />
#### SPARK ecosystem 
![image](https://github.com/user-attachments/assets/b5b24a07-1798-4060-a5d3-008fed93fb83)   
spark itself is not managing the storage and container part. Spark is managing the data processing workload. And that part is managed by the Spark Compute Engine. So the compute engine is responsible for a bunch of things. For example, breaking your data processing work into smaller tasks, scheduling those tasks on the cluster for parallel execution, providing data to these tasks, managing and monitoring those tasks, provide you fault tolerance when a job fails. And to do all these, the core engine is also responsible for interacting with the cluster manager and the data storage manager. So the Spark compute engine is the core that runs and manages your data processing work and provides you with a seamless experience. All you need to do is submit your data processing jobs to Spark, and the Spark core will take care of everything else.  
The second part of the Spark Core

The Core APIs. - This layer is the programming interface layer that offers you the core APIs in four major languages.

Scala,

Java,

Python,

and R programming language.

These are the APIs that we used to write data processing logic during the initial days of Apache Spark.However, these APIs were based on resilient distributed datasets (RDD). But now only a small group is using that.
The topmost layer is the prime area of interest for most Spark developers and data scientists. This layer is again a set of libraries, packages, APIs, and DSL. These are developed by the Spark community over and above the Core APIs. So, you will be using these top-level APIs and DSLs. But internally, all of those will be using Spark Core APIs, and ultimately things will go to the Spark Compute Engine. 


The topmost API layer is grouped into four categories to support four different data processing requirements. However, this is just a logical grouping, and there is no rigid boundary. Things are going to overlap in most of the real-life projects.  
The first group is a set of two things. Spark SQL and then Spark DataFrame/Dataset APIs Spark SQL allows you to use SQL queries to process your data.So that part is quite simple for those who already know SQL. Spark DataFrame/DataSet will allow you to use functional programming techniques to solve your data crunching problems. These APIs are available in Java, Scala, and Python. Both of these together can help you resolve most of the structured and semistructured data crunching problems.   

The next one is Spark Streaming libraries, and they allow you to process a continuous and unbounded stream of data.  

Then you have a set of libraries specifically designed to meet your machine learning, deep learning, and AI requirements.   

The last collection is for Graph Processing libraries and they allow you to implement Graph Processing Algorithms using Apache Spark.So the topmost layer is nothing but a set of libraries and DSLs to help you solve your data crunching problems.  
#### DataBricks
 It is built on top of pyspark, Databricks also offers you an integrated Hive meta-store to store metadata, allowing you to create Databases, Tables, and Views using Spark SQL. On top of this, Databricks also offers you an advanced SQL query engine called Photon, which allows you to gain data warehouse grade performance of your SQL queries and dashboards on top of the data lake infrastructure. Databricks Cloud offers seamless Delta Lake integration that offers ACID transactions and Data consistency features to your Application workload and Spark SQL. Databricks also offers ML Flow which allows us to manage the machine learning life cycle,including experimentation, deployment, model registry, etc
 #### Database VS Dataframe
 Databases offer two things on a high level - Tables and SQL.   
 A database table allows you to load the data in the table.  The data in the table is internally stored as a .dbf file which are stored on the disk. Along with this, the table is made up of "Table Schema" which is basically the list of column names and data types. The schema information is stored in a database data dictionary or a metadata store. This is how a table is organized. We have three layers to form a table. Storage layer stores the table data in a file. Metadata layer stores the table schema and other important information The Logical layer presents you with a database table, and you can execute SQL queries on the logical table.  When you submit a SQL query to your database SQL Engine, the database will refer to the metadata store for parsing your SQL queries. And the database will throw a syntax error or an analysis error if you are using a column name in your SQL that does not exist in the metadata store.  
 
 Apache spark offers you two ways data processing.  
 1. spark database and SQL
 2. Spark Dataframe and API.  
The first approach{Spark database and SQL} is precisely the same as a typical database.  So you will create table and load data into the table. Spark table data is internally stored in the data files. But these files are not dbf files. Spark gives you the flexibility to choose the file format and supports many file formats such as the following.{CSV, JSON, Parquet, AVRO, XML and many more}. Spark supports structured, semi-structured, and unstructured data and distributed storage. Spark also has a metadata store for storing table schema information. So that part is similar to the databases. Then Spark also comes with an SQL query engine and supports standard SQL syntax for processing and querying data from Spark tables.  
Spark goes beyond the Tables and SQL to offer Spark Dataframe and Dataframe API.  
Spark Dataframe is structurally the same as the table. However, it does not store any schema information in the metadata store. Instead, we have a runtime metadata catalog to store the dataframe schema information. The catalog is similar to the metadata store, but Spark will create it at the runtime to store schema information in the catalog. This catalog is only valid until your application is running. Spark will delete this catalog when your Spark application terminates.  
Stark Dataframe is a runtime object and Spark Dataframe supports schema-on-read. Once your program terminates, your dataframe is gone. It is an in-memory object, unlike Spark tables which are permanent.Once created, you will have a table forever. You can drop a table and remove it. But it remains in the system until you drop the table. However, Spark Dataframe is a runtime and temporary object
which lives in Spark memory and goes away when the application terminates. So the metadata is also stored in the temporary metadata catalog.  
The second reason is due to the schema-on-read feature. Spark Dataframe is designed to support the idea of schema-on-read.  Dataframe does not have a fixed and predefined schema stored in the metadata store. Instead, we define the schema when we want to read the data from a file and load it into the Dataframe.  
You can use SQL on the table. However, Dataframe does not support SQL expressions.You must use Dataframe APIs to process data from a Dataframe. Since the table and dataframe are structurally the same, you can convert them to each other. You have two ways of processing data in Spark. And you can use both at your convenience.You can create a table and use SQL or convert a table into a Dataframe and use Dataframe API on the same table.


![image](https://github.com/user-attachments/assets/c9327d7a-9fdf-4ee8-bf58-1e8cd7972878)  

![image](https://github.com/user-attachments/assets/82e6e046-084a-430b-8148-774a01d39cde)  

The spark here is a Spark Session object. Spark session is your entry point for the Spark programming APIs. Every Spark program starts with the Spark Session because Spark APIs are available to you via the Spark Session object.
 Also you cana create global temp tables views from the data frame and then run sql queries on the view  
 ![image](https://github.com/user-attachments/assets/ac0c797c-64de-402e-a760-59da4c3ac909)

 

  


