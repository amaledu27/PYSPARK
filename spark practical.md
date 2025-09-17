# Reading data in to Databrickts
To See or remove files in FileStore, you need to first type %fs and then on the next line type commands like ls <path>
fs stands for filesystem, and if you want to access it directly, then you have to use dbutil.fs (need to check to confirm)
Core structure 
![image](https://github.com/amaledu27/notes/assets/121364324/609289dd-42a0-4aac-8da7-ce1c567ea15d)
![image](https://github.com/amaledu27/notes/assets/121364324/f8170d9c-fe6b-46d7-b210-d4b50b9573c7)

format is optional and default is parquet (can be csv, JSON, JDBC/ODBC, Table, Parquet etc)
Option is also optional and values can be Inferschema, mode, header etc, this is in key value pair format.
Schema is the schema of the data which you can pass if you want, this is also optional.
Load is the path where our data is loaded from, this is the only mandatory field.

How to access dataframe reader API ?
When you use or (initiate i guess) the spark session by spark.read, the dataframe API will become active.
Here is an example, here, inferschema is true (ie automatically get schema from the data loaded) SO Schema option is not used.
![image](https://github.com/amaledu27/notes/assets/121364324/6a3a8004-d4c3-4432-95a3-6172d239222e)

Below are the three modes of reading data, permissive mode will set the corrupt values to null. in DROPMALFORMED< if there is blanks, it looks like it will put null and if structure cannot be comprehend, then it will drop(need to confirm the null part)
![image](https://github.com/amaledu27/notes/assets/121364324/5c7d442d-0f83-4a93-be2c-21ac337ded02)

example from databricks
------------------------
~~~
flight_df_header = spark.read.format("csv")\
        .option("header","true")\
        .option("inferschema","true")\
        .option("mode","FAILFAST")\
        .load("/FileStore/tables/flight_data.csv")
flight_df_header.show(5)



flight_df_header:pyspark.sql.dataframe.DataFrame = [DEST_COUNTRY_NAME: string, ORIGIN_COUNTRY_NAME: string ... 1 more field]

+-----------------+-------------------+-----+
|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
+-----------------+-------------------+-----+
|    United States|            Romania|    1|
|    United States|            Ireland|  264|
|    United States|              India|   69|
|            Egypt|      United States|   24|
|Equatorial Guinea|      United States|    1|
+-----------------+-------------------+-----+
only showing top 5 rows

flight_df_header.printSchema()
root
 |-- DEST_COUNTRY_NAME: string (nullable = true)
 |-- ORIGIN_COUNTRY_NAME: string (nullable = true)
 |-- count: integer (nullable = true)
~~~
# how to creat schema manually

There are two ways,
1)Struct type and struct field
2)DDL

to use the first method, we have to import "struct type" and struct field" as below
~~~
from pyspark.sql.types import structtype,structfield
~~~
Both are CLASSES defined in 
Struct Type defines our structure of Dataframe.(it is the list of struct fileds)
Struct Field is the details of each column, ie the data type, if it can be null etc.
while defining schema, Struct Fields are considered as lists ie listed in "[]"

![image](https://github.com/amaledu27/notes/assets/121364324/e8ba2a57-cab9-4877-932b-823a3af730e1)


The second method is DDL, which is the easier method, where we pass on a string in which there will be the name and  data types of each column
~~~
from pyspark.sql.types import StructField,StructType, StringType,IntegerType

my_schema= StructType(
    [StructField("DEST_COUNTRY_NAME",StringType(), True),
                 StructField("ORIGIN_COUNTRY_NAME",StringType(), True),
                 StructField("count",IntegerType(),True)
    ]
)

flight_df = spark.read.format("csv")\
        .option("header","false")\
        .option("skipRows",1)\
        .option("inferschema","false")\
        .schema(my_schema)\
        .option("mode","FAILFAST")\
        .load("/FileStore/tables/flight_data.csv")
flight_df.show(5)
(1) Spark Jobs
 
flight_df:pyspark.sql.dataframe.DataFrame = [DEST_COUNTRY_NAME: string, ORIGIN_COUNTRY_NAME: string ... 1 more field]
+-----------------+-------------------+-----+
|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
+-----------------+-------------------+-----+
|    United States|            Romania|    1|
|    United States|            Ireland|  264|
|    United States|              India|   69|
|            Egypt|      United States|   24|
|Equatorial Guinea|      United States|    1|
+-----------------+-------------------+-----+
only showing top 5 rows
~~~
In the example above, we had 2 options, one is to change mode to Permissive, as the first row had null value, which would have given an error 
the next option is to use skipRows, by which we can skip the 1st raw and tkeep the mode as FAILFAST

## Link for SPARK documentation for CSV
https://spark.apache.org/docs/latest/sql-data-sources-csv.html

## Handling corrupt records
Such things usually happens with CSV files or JSON, In JSON, usually the } will be missing and in CSV files there will be an extra value ( eg state, Kerala,india) 
###Printing corrupt data after saving to a file
You will hav to create a new schema to include corrupt records.

~~~

emp_schema= StructType(
    [StructField("id",IntegerType(), True),
                 StructField("name",StringType(), True),
                 StructField("age",IntegerType(), True),
                 StructField("salary",IntegerType(), True),
                 StructField("address",StringType(), True),
                 StructField("nominee",StringType(), True),
                 StructField("_corrupt_record",StringType(), True)
    ]
)
-----
employee_df = spark.read.format("csv")\
    .option("header","true")\
    .option("inferschema","false")\
    .schema(emp_schema)\
    .option("mode", "PERMISSIVE")\
    .load("/FileStore/tables/employee_file.csv")
employee_df.show(truncate =False)

employee_df:pyspark.sql.dataframe.DataFrame = [id: integer, name: string ... 5 more fields]
+---+--------+---+------+------------+--------+-------------------------------------------+
|id |name    |age|salary|address     |nominee |_corrupt_record                            |
+---+--------+---+------+------------+--------+-------------------------------------------+
|1  |Manish  |26 |75000 |bihar       |nominee1|null                                       |
|2  |Nikita  |23 |100000|uttarpradesh|nominee2|null                                       |
|3  |Pritam  |22 |150000|Bangalore   |India   |3,Pritam,22,150000,Bangalore,India,nominee3|
|4  |Prantosh|17 |200000|Kolkata     |India   |4,Prantosh,17,200000,Kolkata,India,nominee4|
|5  |Vikash  |31 |300000|null        |nominee5|null                                       |
+---+--------+---+------+------------+--------+-------------------------------------------+
~~~


##Storing corrupt records
the corrupt records will be stored in JSON format and you should not mention mode when storing corrupt records as its selected automatically

~~~

employee_df = spark.read.format("csv")\
    .option("header","true")\
    .option("inferschema","false")\
    .schema(emp_schema)\
    .option("badRecordsPath", "/FileStore/tables/bad_records")\
    .load("/FileStore/tables/employee_file.csv")
employee_df.show(truncate =False)
~~~
Now you need to locate the JSON file from the location given(check this as there seems to be a confusion regarding the location is looping a bit) and then check the corrupted data
~~~
bad_data_df= spark.read.format("json").load("/FileStore/tables/bad_records/20240603T195019/bad_records/part-00000-7f85110b-bda5-4726-baed-a6789d2e6f7b")
bad_data_df.show(truncate = False)

(2) Spark Jobs
 
bad_data_df:pyspark.sql.dataframe.DataFrame = [path: string, reason: string ... 1 more field]
+----------------------------------------+--------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------+
|path                                    |reason                                                                                                                          |record                                     |
+----------------------------------------+--------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------+
|dbfs:/FileStore/tables/employee_file.csv|org.apache.spark.SparkRuntimeException: [MALFORMED_CSV_RECORD] Malformed CSV record: 3,Pritam,22,150000,Bangalore,India,nominee3|3,Pritam,22,150000,Bangalore,India,nominee3|
|dbfs:/FileStore/tables/employee_file.csv|org.apache.spark.SparkRuntimeException: [MALFORMED_CSV_RECORD] Malformed CSV record: 4,Prantosh,17,200000,Kolkata,India,nominee4|4,Prantosh,17,200000,Kolkata,India,nominee4|
+----------------------------------------+--------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------+

~~~

# Reading JSON in spark
JSON - JavaScript Object Notation, used for storing semi structured data, data is stored in key value pairs.
Suppose these is atable of employees with id, name and age, then in Jason, it can stored as 

{"id":1, "name": "sasi", "age : 23}
{"id":2, "name": "ravi", "age : 43}
Suppose, we have to sotore nominee info in row 1 and  salary info in row 2, we can just add it as key value pairs, without affecting structure of any other rows. It it was tstored in tabular structure, it would have needed adding extra column for each row.
####When there is the extra information in one of the entries, then SPARK will automatically create extra column for the extra value and will put NULL for all the entries where that value is not available.
There is "Single line JSON" and "Multiline Jason" Sigle line JSON gives bettter performance and when reading data you have to mention which type it is and you have to pass multiline as list of dictionaries [{},{}]
else it will just read the first entry and will give error if multilne true option is not present
~~~
spark.read.format("json")\
    .option("inferSchema", "true")\
    .option("mode", "PERMISSIVE")\
    .option("multiline", "true")\
    .load("/FileStore/tables/Multi_line_incorrect.json").show()

~~~
![image](https://github.com/amaledu27/notes/assets/121364324/85386c8f-979a-42f6-9c42-73c0483f35c7)

Suppose there a corrupdated data, say due to missing end }, then when reading data, spark will automatically create a corrupt record column and will put the data there . But it is an ivalid file, then you will get the error
When JSON values are NESTED, you will have to flatten the file to data frame

# Apache Parquet

Parquet is a hybrid columnar based file format ,It is STRUCTURED file format stored in Binary form.
The difference is in how it stores data in disk.
![image](https://github.com/amaledu27/notes/assets/121364324/511b684d-c209-4259-bd99-97c3c63d2116)

Parquet is mainly used in Big data,( write once read many where only some columns are required mostly, OLTP)
OLAP - OnLine Analytical Processing
OLTP - OnLine Transactional Processing, where writing (insert, update, delete etc, like banking system)Here ROW based file formatiting is easier, as it will be accessing row based info.

The idea is to select a format that will give continious data reading, resulting hih perfomance and low cost.

How wever columnar storing has some issues as well. To solve this we use hybrid storage where data is divded based on row and then used columnar storage.(chunk based columnar)

![image](https://github.com/amaledu27/notes/assets/121364324/a2ebce53-4afe-4477-b832-7a050a04cf40)


![image](https://github.com/amaledu27/notes/assets/121364324/c70edc37-51fc-4e57-b147-cc87c51e7cb8)
Each divided chunk is known as ROW GROUP, each RAW Group is divided by 128 MB by default
Treestructure of each RAW group is shown below.
In each ROW Group, there will be a seperate selction for each column.There will be a PAGE section for each column and it will have all the data values and metadata, including min and max values.
![image](https://github.com/amaledu27/notes/assets/121364324/e9f30f1b-61a4-4880-8110-123d87480a92)
#### optimization while searching / queries 
Parquest will have ROW GROUP LEVEL METADATA and it will do the following while searching.
Predicate Pushdown - based on meta data, only required raw groups were used (row group based data selection as needed. 
Projection Pruning - The columns that are not required will be discarded based on metadata of parquet
get more infofor the two mentioned
### copied from youtube
(
Download Parquet Data:- https://github.com/databricks/Spark-T...

Download parquet tools in your local to run all the below commands.
Parquet tools can be downloaded using pip command.
Run the below command in cmd or terminal
pip install parquet-tools

Run the blow command inside python
import pyarrow as pa
import pyarrow.parquet as pq

parquet_file = pq.ParquetFile(r'C:\Users\nikita\Downloads\Spark-The-Definitive-Guide-master\data\flight-data\parquet\2010-summary.parquet\part-r-00000-1a9822ba-b8fb-4d8e-844a-ea30d0801b9e.gz.parquet')
parquet_file.metadata
parquet_file.metadata.row_group(0) 
parquet_file.metadata.row_group(0).column(0)
parquet_file.metadata.row_group(0).column(0).statistics 

Run the below command in cmd/terminal
parquet-tools show  C:\Users\manish\Downloads\Spark-The-Definitive-Guide-master\data\flight-data\parquet\2010-summary.parquet\part-r-00000-1a9822ba-b8fb-4d8e-844a-ea30d0801b9e.gz.parquet
parquet-tools inspect  (path of your file location as above)
https://parquet.apache.org/docs/file-format/metadata/

)
Metadata example
![image](https://github.com/amaledu27/notes/assets/121364324/f6219b4c-8f37-4bfe-8468-4208b1ec062d)

RLE - RUN LENGHTH ENCODING (  example 0,1,2,3,3,3,1,0 can be written as 0,1,2,(3,3),1,0)
BIT PACKING (storage pomression done by selecting  small data type as needed
COMPRESSION > GZIP, will provide much faster response 

![image](https://github.com/amaledu27/notes/assets/121364324/9be31765-d5e2-4c22-bc45-56ba80dab726)

# Writing  dataframe to disk

We connect to different data, by uploading to databricks or connecting to different databases or sources and then transform it, and the data will still be in memory, now we need to write it to disk.
![image](https://github.com/amaledu27/notes/assets/121364324/dbb54948-6ef5-4a9d-ae41-d3c54cc16872)
format is a method
![image](https://github.com/amaledu27/notes/assets/121364324/b513b57c-5ec0-45e5-824a-c755e0e5837d)

Write modes>
![image](https://github.com/amaledu27/notes/assets/121364324/665b8eb4-38d4-4c75-8300-da15727ed1be)

~~~
df.write.format("csv")\
    .option("header", "true")\
    .option("mode","overwrite")\
    .option("path", "/FileStore/tables/csv_write/")\
    .save("<givelocation>")

If you wanted to do repartition 3 times, first line should be df.repartition(write.format("csv")\
~~~
To see the written file -
~~~
dbutils.fs.ls("/FileStore/tables/csv_write/")

Out[5]: [FileInfo(path='dbfs:/FileStore/tables/csv_write/_SUCCESS', name='_SUCCESS', size=0, modificationTime=1717470823000),
 FileInfo(path='dbfs:/FileStore/tables/csv_write/_committed_1285195479545954089', name='_committed_1285195479545954089', size=111, modificationTime=1717470823000),
 FileInfo(path='dbfs:/FileStore/tables/csv_write/_started_1285195479545954089', name='_started_1285195479545954089', size=0, modificationTime=1717470822000),
 FileInfo(path='dbfs:/FileStore/tables/csv_write/part-00000-tid-1285195479545954089-e2059646-854e-4875-9d58-09ac64c913f7-3-1-c000.csv', name='part-00000-tid-1285195479545954089-e2059646-854e-4875-9d58-09ac64c913f7-3-1-c000.csv', size=490, modificationTime=1717470822000)]
~~~
### Partitioning and bucketing
Eventhough during initial storage in parquet format, data is divided in to chunks, when we write data back to disk, there is Partitioning and bucketing we can do.
Bucketing VS Partitioning
Partitioning is used where we can categorise data based on one column value, (address here ) and then divie it in to chunks based on that.
Bucketing is used when there are no such columns, then we ask bucketing to be done with something that can be sored.
![image](https://github.com/amaledu27/notes/assets/121364324/4e965ae3-9bab-4b74-9621-8f361ed1d14f)

~~~
writing by partition

df.write.format("csv")\
    .option("header","true")\
    .option("mode","overwrite")\
    .option("path", "/FileStore/tables/CSV_write/")\
    .partitionBy("address")\
    .save()

checking written data on the given path.

dbutils.fs.ls("/FileStore/tables/CSV_write/")

Out[5]: [FileInfo(path='dbfs:/FileStore/tables/CSV_write/_SUCCESS', name='_SUCCESS', size=0, modificationTime=1717508247000),
 FileInfo(path='dbfs:/FileStore/tables/CSV_write/address=INDIA/', name='address=INDIA/', size=0, modificationTime=0),
 FileInfo(path='dbfs:/FileStore/tables/CSV_write/address=JAPAN/', name='address=JAPAN/', size=0, modificationTime=0),
 FileInfo(path='dbfs:/FileStore/tables/CSV_write/address=RUSSIA/', name='address=RUSSIA/', size=0, modificationTime=0),
 FileInfo(path='dbfs:/FileStore/tables/CSV_write/address=USA/', name='address=USA/', size=0, modificationTime=0)]

~~~

If there is no cardinality and partitioning with one column will create too many chunks, then Bucketing will be a better idea.
While partitioning, we acan further seggregate the data based on other columns, then we need to list it in the partitionBy option with the desired logical order. When you list what is written, it will show the pirst partition option only, but within that there will be subfolders with sub partitions. Note that the output is filesystem not tables.

When opting for bucketing, the instead of .save, you have to use saveAsTable option(and then the details will go to the hive metastore, check more details), and in.bucket, you need to also mention the number of buckets as well(along with the column).

~~~
df.write.format("csv")\
    .option("header","true")\
    .option("mode","overwrite")\
    .option("path", "/FileStore/tables/bucket_by_id/")\
    .bucketBy(3,"id")\
    .saveAsTable("bucket_by_id_table")

checking written data

dbutils.fs.ls("/FileStore/tables/bucket_by_id/")

Out[5]: [FileInfo(path='dbfs:/FileStore/tables/bucket_by_id/_SUCCESS', name='_SUCCESS', size=0, modificationTime=1717513674000),
 FileInfo(path='dbfs:/FileStore/tables/bucket_by_id/_committed_47407619679420805', name='_committed_47407619679420805', size=297, modificationTime=1717513674000),
 FileInfo(path='dbfs:/FileStore/tables/bucket_by_id/_started_47407619679420805', name='_started_47407619679420805', size=0, modificationTime=1717513672000),
 FileInfo(path='dbfs:/FileStore/tables/bucket_by_id/part-00000-tid-47407619679420805-ce2f2d6e-f1fd-497c-a25c-1254ae9bd411-2-1_00000.c000.csv', name='part-00000-tid-47407619679420805-ce2f2d6e-f1fd-497c-a25c-1254ae9bd411-2-1_00000.c000.csv', size=270, modificationTime=1717513672000),
 FileInfo(path='dbfs:/FileStore/tables/bucket_by_id/part-00000-tid-47407619679420805-ce2f2d6e-f1fd-497c-a25c-1254ae9bd411-2-2_00001.c000.csv', name='part-00000-tid-47407619679420805-ce2f2d6e-f1fd-497c-a25c-1254ae9bd411-2-2_00001.c000.csv', size=113, modificationTime=1717513673000),
 FileInfo(path='dbfs:/FileStore/tables/bucket_by_id/part-00000-tid-47407619679420805-ce2f2d6e-f1fd-497c-a25c-1254ae9bd411-2-3_00002.c000.csv', name='part-00000-tid-47407619679420805-ce2f2d6e-f1fd-497c-a25c-1254ae9bd411-2-3_00002.c000.csv', size=115, modificationTime=1717513673000)]

There is task id which is same for each bucket > 00000-tid-47407619679420805
there there are 3 buckets as requested > 1254ae9bd411-2-1_00000.c000.csv
~~~

The only possible problem with this is that if there are 200 tasks , bucketing will create 200 X 3 buckets, to solve this, we have to do df.repartition(3) first and then do the bucketing.
Bucketing is mainly done to avoid shuffling, which will cause high cost 
For this to happen effectively, when there are two tables which we know that will be used for joining, we should ideally have same buckets and  almost same id's so that it can be joined easily.


# Creating dataframe API
![image](https://github.com/amaledu27/notes/assets/121364324/db907303-c6a4-40e6-8f15-13d54bc0ad35)

in the DE pipeline, there is read, transform and write, then that data is used by ML or BI people. 
There are two ways to transform data, using Dataframe API and Spark SQL(spark SQL is sql itself). We are going to focus on Dataframe API

SCHEMA

![image](https://github.com/amaledu27/notes/assets/121364324/6a86d333-cd21-435b-bab7-91cf4a003a41)

![image](https://github.com/amaledu27/notes/assets/121364324/d3fd14b6-6907-4671-b8d9-050b36fd57b7)
Nullable = true, meaning the value can be null.

if you only need list of columns, then use > employee_df.columns.

dataframe is t made up of columns and rows. Data frame gets stored in memory as a Row() type object.
![image](https://github.com/amaledu27/notes/assets/121364324/17aa69e4-c4fa-42ae-a9ad-286cd7835e42)

Creating rows, which is usually not required.
![image](https://github.com/amaledu27/notes/assets/121364324/82ecf224-cf1c-4e8e-ad2e-0d01b7d3cfbe)

Columns are expressions(set of transformations on one or more than one values in a record.)
example > df.select(col("age") + 5)
there are different methods to select just one (or more) columns
employee_df.select("name").show()

or 
employee_df.select(col(("name")).show()
you have to import the following before running the col function
~~~
from pyspark.sql.functions import *
from pyspark.sql.types import *
~~~
when using the col method it explicity tells that it is a column, so that we can modify the column in single line
employee_df.select(col("id") + 5).show()

here is an example of multiple methods to call many columns

~~~
employee_df.select("id",col("name"),employee_df["salary"],employee_df.address).show()

(1) Spark Jobs

+---+--------+------+------------+
| id|    name|salary|     address|
+---+--------+------+------------+
|  1|  Manish| 75000|       bihar|
|  2|  Nikita|100000|uttarpradesh|
|  3|  Pritam|150000|   Bangalore|
|  4|Prantosh|200000|     Kolkata|
|  5|  Vikash|300000|        null|
+---+--------+------+------------+
~~~

##### expr function 
it will look at the given info and then will look for collumn ids and then will convert it to column method and then will do the experssion(like addition) and then will return it to select method

>>> It can also be used to put sql statements like concat
 employee_df.select(expr("id as employee_id"), expr("concat(name,address)")).show()
(1) Spark Jobs
 
+-----------+---------------------+
|employee_id|concat(name, address)|
+-----------+---------------------+
|          1|          Manishbihar|
|          2|   Nikitauttarpradesh|
|          3|      PritamBangalore|
|          4|      PrantoshKolkata|
|          5|                 null|
+-----------+---------------------+

Writing the code in SPARKSQL
to do this we have to convert the df to a temporary (virtual)view as below 
employee_df.createOrReplaceTempView("employee_tbl")

then write write spark.sql then in brackets write """ option to write multiline code


employee_df.createOrReplaceTempView("employee_tbl")

~~~
spark.sql('''
   select * from employee_tbl       
          
          ''').show()

+---+------+---+------+------------+--------+
| id|  name|age|salary|     address| nominee|
+---+------+---+------+------------+--------+
|  1|Manish| 26| 75000|       bihar|nominee1|
|  2|Nikita| 23|100000|uttarpradesh|nominee2|
|  5|Vikash| 31|300000|        null|nominee5|
+---+------+---+------+------------+--------+

~~~

To Select all the columns from using pyspark you can 
employee_df.select("*").show()
