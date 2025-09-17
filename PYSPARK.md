Spark is distributed coomputing engine which distributes our data to process it.  
![image](https://github.com/user-attachments/assets/3b3d14f4-83da-4583-b822-0eadd1fd2406)

There is a Cluster manager in every cluster, which will create Driver node {node - which is replacable} in our cluster. When a code is submittted the code, the DRIVER program will assess the code and will assess the code with transformations, stages, jobs and tasks and then will transfet the info to the cluster manager.The driver program will request the cluster manager for the number of worker nodes required.Then all the breakdown info  provided by the driver program will be moved to thsese workers. Then the workers will execute it and process the data.Here the Driver node will act as the master and the worker node will act as the slaves.
![image](https://github.com/user-attachments/assets/e83c3682-b36e-4caa-a936-120d3d2503f0)
In_Memory Computation - Unlike hadoop which writes the data to the disk for processing, in spark dtat processing is done in Memory, which gives much faster results.
Lazy evaluation  - when it comes to transformations, Spark will not immediatly execute those transformations, instead it will create a logical plan and create a lazy evaulation and will execute the evaluations only when we trigger a action.  {transformation can be sorting, selecting or even adding columns, in the logical plan, spark will change the order for efficiency}
FAULT TOLERANT - we can traceback all the actions that were done in spark
#### Heirarchical structure of the code  / transformations  
The code that we give in a cell is called the JOB. Each job will be divided to multiple stages, and each stage will have multiple tasks.  
![image](https://github.com/user-attachments/assets/bcc2e9c9-18ef-4571-bc27-99602090fd21)  
Code to read data that is stored in catalog   
~~~
df = spark.read.format('csv').option('inferSchema',True).option('header',True).load('/FileStore/tables/BigMart_Sales.csv')
df.display()
~~~
to see the data in dataframe use the df.display or df.show, you may also add the number of rows that you want to see.
When you are uploading json, there can be single line version or multilive version which is heirarchical, so we need to mention that as well.  
~~~
df_json = spark.read.format('json')\
    .option('inferSchema',True)\
    .option('header',True)\
    .option('multiline',False)\
    .load('/FileStore/tables/drivers.json')
~~~
### Defining Schema - DDL and StructType()
When we dont want spark to automatically infer the schema, we have to define the schema
To See the current schema of an existing df, use the command 
~~~
df.printSchema()
~~~

The DLL command is almost similar as sql. As there are multiple lines and we need to write it in multiple lines youneed to use triple quotes.  
![image](https://github.com/user-attachments/assets/1df3f0c4-9bbb-499f-902e-7fd6106e6321)  
Writing in parquet format is also in same way. the new DELTA format also 
![image](https://github.com/user-attachments/assets/fa2fb0dc-ba84-41c3-9530-dc26ac76ee0c)


Now lets see the second way, using the structtype, to do this you need to import sql types structtype and structfield., To do so, you may simply import sql.types and sql.functions.  
When you define, the schema, you may define it inside structtype, and inside that using structfiled as a list, with each column will be a part of the list.  
When defining, inside the structfiled, mention the column name is single quotes,  followed by the datatype of the column{see below} and then a true / false based on if the value is nullable or  not

![image](https://github.com/user-attachments/assets/161fd610-bd93-443c-a7fa-f4acbc49b710)

![image](https://github.com/user-attachments/assets/04ac4566-6b56-4424-b931-965d52ae89ec)   
![image](https://github.com/user-attachments/assets/a7741d6f-4e48-450e-a3c3-282dc9234497)
#### SELECT
We can use select with the same concept as that of the sql, eventhough the syntax is different. Infact there are many ways in which SELECT can be used in PYSPARK  
We can simply put the column names or you can you the standardised way in which you have to put use col and the column name inside that. Also when ever you want to apply a column object, you need to use the col object.
![image](https://github.com/user-attachments/assets/1b9d48ea-ad24-4e68-ba70-0ef595e561d9)  
Note that in the example below, you are using the display function to dispaly the columns, but if you want to use it, you need to store it to a dataframe.
#### ALIAS
used to rename the column  
![image](https://github.com/user-attachments/assets/b47fbc14-7635-4971-ad19-9afb8ab7de50)
#### FILTER / WHERE
![image](https://github.com/user-attachments/assets/bfe439ed-38dc-477e-ba51-a582638eb016)  
We can also do filterring using multiple conditions, you just need to encapsulate each condetion between and or or so that the engine can clearly identify it.  
![image](https://github.com/user-attachments/assets/659503ba-9891-41d4-8d85-8ed409122256)

In the thirdone, we will see isNull  and isin functions being applied to the columns.  
![image](https://github.com/user-attachments/assets/13e57b2e-263e-4ee8-8c94-9b3d051a3bbf)  
#### withColumnRenamed.
This lets you rename the column in dataframe level.  current name should be mentioned first forllowed by the second name.
![image](https://github.com/user-attachments/assets/439c2c66-c868-415d-8986-754b986f9a09)  

#### WithColumn
Lets you create newccolumn or modify the existing coluumn

To create a new column and to add a constant value on that column, you need to use lit function
![image](https://github.com/user-attachments/assets/a8ea50cc-ff52-438e-9512-830be2691077)

Now if we are using two existing columns, we can use the withcolumn function to get a new column as below  
![image](https://github.com/user-attachments/assets/0ab88bc8-6ac4-46f2-af8b-44b0f67dd601)  
If we want to updte the exisiting column, then following syntax; you have to use the regexp_replace function here to replace the existing ones with the new ones  
![image](https://github.com/user-attachments/assets/030ac06c-4778-4d1a-90d0-a3ea0aa6e8c5)

If yougive a new name , then that will be added as a new column and if you  give name of the  existing column, it will replace the existing column with the new value.

#### Typecasting 
to change datatype - example int to string  
![image](https://github.com/user-attachments/assets/b438866a-e5e4-4a4b-bf71-7f3ac5402526)  
Please note that you yoused brackets with stringtype as a function or API(need to check which type}

#### Sort/orderBy
After mentioning the column name, mention in which order{ascending / descending} you like to have it sorted  
![image](https://github.com/user-attachments/assets/40ea25fb-aff2-40d1-8dcd-ca5a5ca7fad3)  

##### Sorting based on Multiple columns

Here we use 0 /1 as false /true to indicate the order of sorting  
![image](https://github.com/user-attachments/assets/92847a20-fece-480c-a30a-dad9135e5831)  
#### limit
same as limit function in sql or pandas. ie to limit the number of rows in the output.  
![image](https://github.com/user-attachments/assets/34c80a47-c2bc-4bdc-8e32-fb0b58dc7d08)
#### DROP
![image](https://github.com/user-attachments/assets/fefad8d9-4a90-4a19-9c62-c3c3793d6705)

#### DROP_DUPLICATES
dropping duplicates can be done based on a single column and DEDUP {meaning  dropping columns where combined rows are resulting in duplicates }
for dedup, there are two options which does the same  

![image](https://github.com/user-attachments/assets/dca04964-dcf8-4f02-b677-9a9b7910c8e2)
Distinct function will also do the same  
![image](https://github.com/user-attachments/assets/e6a1a436-3f3c-4590-90e3-7cf1c16fb08f)


if it is subset column based only
![image](https://github.com/user-attachments/assets/7a487099-a019-4dc3-9329-c58ec32a78c0)  
#### Union function
Union function will combine two dataframes.The simple union will join the two datframes irrespective of the column names    
![image](https://github.com/user-attachments/assets/820f8128-168b-49ae-85c4-6e7c29d1b826)  
If the column name was in different order, we can use "union by name",

![image](https://github.com/user-attachments/assets/33187af1-778c-4d4c-8750-35cb4688a4eb)  
#### String functions {INITCAP, UPPER, LOWER}
![image](https://github.com/user-attachments/assets/aacc8a1c-61b3-4131-b5cc-bbc5ca18fd13)  
![image](https://github.com/user-attachments/assets/b7a43005-3f6a-4586-9e05-43b04d9b35b7)
Syntax for lower is also the same.
#### DATE FUNCTIONS
CURRENT_DATE, DATE_ADD and DATE_SUB
To get current date use the function below, see that there is an option for timestamp as well.  
Also note the use of "withColumn" to create a new column  
![image](https://github.com/user-attachments/assets/fd2c2773-d861-47f3-937e-38f0e7744028)

We can add dates to the current date or any other date that is on a column using the "date add"  
![image](https://github.com/user-attachments/assets/7197f0b4-2f83-4a8a-890c-a27aa6ea07b1)
if you want to substract a number of days from a column, then use "date sub"  or you can use data add functin with number odf dates as minus. example : -7
![image](https://github.com/user-attachments/assets/94b413b3-9ecd-41f0-8e28-2149b99e2f5d)
 ##### DATEDIFF
 used to find the difference between two dates.  
 ![image](https://github.com/user-attachments/assets/283b01b2-192b-40ef-8e90-3d15335fae11)  
 ##### DATE FORMAT
using date format, you can change the format of the date.  
![image](https://github.com/user-attachments/assets/a9b12921-9c49-4b47-9158-7f161e94f623)  
In the example above, we are trying to change an exisiting column date format. as per the feedback from databricks , we need to give the date format in string format  
![image](https://github.com/user-attachments/assets/3f80ae8b-2074-4249-8dec-ebb8b0398d7a)
#### HNDLING NULL VALUES

Main two options are dropping nulls and filling nulls

![image](https://github.com/user-attachments/assets/aac6a1de-1240-4747-ad06-e3c2b68fa3ca)  
The dropna function has different options on how to operate,  
any - drops the record{row} if any columns in it are null   
all drops the record if all columns are null   
![image](https://github.com/user-attachments/assets/80d53c88-7014-4b7a-96f3-a36a4c2614d6)  
![image](https://github.com/user-attachments/assets/5fea9d21-cfb9-42ee-a871-29530ad33c67)  
if we want to limit the nullcheck to only one column, then use the subsetoption.  
![image](https://github.com/user-attachments/assets/91c9e216-f334-4f29-93ba-af3fd35b7934)

##### filling Nulls
![image](https://github.com/user-attachments/assets/10f3de25-7b7d-43b7-aaac-705c83f8dec5)  
IN example above, it will fill all null values with the value that we provide.  
We can use seubset to  replace null values in a particular column only.  
![image](https://github.com/user-attachments/assets/1ac00e5d-1c40-4bd5-ba47-9c8b8d28670c)  
#### SPLIT and INDEXING function
For splitting, you need to mention the column name that you want to do the split, the delimeter for splitting decision. Also, as you are plaaning to create a new column, you need to mention  the "withColumn" as well.  
![image](https://github.com/user-attachments/assets/f50bf4dc-bca8-4723-b5ed-291e4d5425ba)  
Once we do the splitting, the values will be stored on the new column as list. We need to use indexing to split it from the list.to limit the new column entry to a particular indexed value, you may use indexing combined with SPLIT function.  
![image](https://github.com/user-attachments/assets/1438f9a1-1f94-45f4-891c-79a581bfbcf8)
![image](https://github.com/user-attachments/assets/b982121f-0af5-4a81-98a5-cc3d9cf7d083)
#### EXPLODE function
In the scenario where we have a list , the EXPLODE function will create a new ROW for each element in the list.
![image](https://github.com/user-attachments/assets/0b29547e-807f-4132-95ab-df3bd5bf2e31)  
![image](https://github.com/user-attachments/assets/d8a03c1e-03d0-4adc-bedd-f9224e09fbbb)  
#### Array contains
Gives you flags {true / false} based on the condition check, ie if the list contains a given value

![image](https://github.com/user-attachments/assets/35fd8c55-ad0e-46fd-a64f-c1b25fecf7ee)  
![image](https://github.com/user-attachments/assets/8a8d6e57-d353-49ba-9c3b-e16779f8ddda)
#### GROUPBY
![image](https://github.com/user-attachments/assets/0ba7a066-8abe-49fe-8003-9ec213fb4ef3)  
Here you have to mention, which column based on which you need to groupby, and then use the aggregation function  
You can group by multiple columns and also do various aggregation functions. You just need to metion them within the paranthesis in the correct order{order matters}  
![image](https://github.com/user-attachments/assets/c22202d6-36a8-410c-81ef-35a923d3a52b)  
#### COLLECTION LIST
If there are two columns and in the second one, there are multiple values rows corresponding to a single value in column 1, and there are n number of similar instances in column 1{this is similar to group concat fn in mysql}  
![image](https://github.com/user-attachments/assets/a82fee4e-7a48-4d6f-84a9-c10dccc5f7b5)  
#### PIVOT
Pivot tables will let you map types of values to be placed at two axises and then map the desired aggregations to provide proper analysis.  
![image](https://github.com/user-attachments/assets/9134a4d0-f896-4fcf-ac00-cc093a0b8573)  
![image](https://github.com/user-attachments/assets/5f56d42a-0cf4-4329-bb44-fe0a2ba2cfdd)  
#### WHEN OTHERWISE
It is same as Case when statement in SQL,(or IF elese in python}  ie to make conditions.   
In the example below, in a new column veg-flag, when Item_Type is meat, the entry will be made for "non-veg" and otherwise , veg.
![image](https://github.com/user-attachments/assets/d63e7fc1-0e42-433f-a56e-abc33cbd08d1)  
You can use multiple "When" conditions together, you just use them using the ".when" without comma  
![image](https://github.com/user-attachments/assets/1fa1d294-8d43-46fc-aa30-36cd9c4fb97f)  
#### JOINS
General structure is , first dataframe.join, in bracket, second df, joining column in square brackets of df from both{df} end{ like sql} , type of join
##### INNER JOIN
only present values that are  in both tables, based on a common column. If there are duplicates, then those will be included  
![image](https://github.com/user-attachments/assets/f5f18705-0e2c-4ec1-83ca-733c554f4d9b)  
##### LEFT JOIN
WIll include all values in the left DF, regardless if the value is in the right side column  
![image](https://github.com/user-attachments/assets/e352f227-e619-406e-ba09-0e8ca6a95fe8)  
##### RIGHT JOIN
WIll include all values in the RIGHT DF, regardless if the value is in the LEFT side column  
![image](https://github.com/user-attachments/assets/ceccd2d4-112f-4391-a6fd-4d4ad9f406de)  
##### ANTI JOIN
its not available in SQL as of now. Syntax is similar to other join types. This is used to get data that is available in one df but not in the other  
![image](https://github.com/user-attachments/assets/fdcf1e59-7508-429a-8ad4-baca2a7e26ea)  
#### WINDOW functions
WIndow functions are special functions which performs row level calculations. example ROW NUMBER, RANK, DENSE RANK, 
##### row_number
This is used to give unique identifier to each value to {over} a specific column, note that here even duplicates will get different unique values.  
![image](https://github.com/user-attachments/assets/50fc25ef-669e-48f3-a054-d1743dc44545)  
##### RANK and DENSERANK
![image](https://github.com/user-attachments/assets/223900e6-4c15-4921-9a89-d72390d988cb)
![image](https://github.com/user-attachments/assets/51c54fd6-de00-4b05-9317-89a397f11d76)  
Note that you can also mention the order in which you wanna rank it {asc or desc}  
below is the example in which you are also applyong denserank along with rank  
![image](https://github.com/user-attachments/assets/8e043e55-5776-40e3-90b4-c94c44655fab)  
##### Cumulative sum

Here apply sum function with window function and we also have to do Frame Clause{we will take sum of all previous records and then will add the current row.}  
In other words, if we just use sum over a column {of a category} it will show total sum on all columns, and will not show the increase per column
![image](https://github.com/user-attachments/assets/33789d97-d518-4678-84c0-22890f211b46)  
So to see the gradual increase, you need to apply frame clause on top of it. We do it inside the OVER function as below with "Window.unboundPreceding,Window.CurrentRow"  
![image](https://github.com/user-attachments/assets/fe246122-71ed-4601-a351-85377a5cbfa2)  
If instead of currentrow, if you used unbounded following, it will again start giving the total sum for the category.  
#### USER DEFINED FUNCTIONS (UDF)
It is not advised eventhough it is possible as it will cause performance issues if coded in python as code needs to be converted for usage in executers that are JVM{java virtual machines}
STep 1 - create function  
SETP 2 - You have to convert it to a pyspark function
then use the UDF  
![image](https://github.com/user-attachments/assets/56bcebe0-e353-4bdb-8de6-a108ab0d1206)  
#### Data Writing.
There are four writing modes  
APPEND - Will just append the data, if there is already a file, it will just create another file if we didnt mention the file name, if we did mention the file name, then the file will be ?  
OVERWRITE - WIll overwrite the existing file, resulting loss of old data.  
ERROR - gives you back an error message if there is already a file in there.   
IGNORE - If there is a file already , it will do nothing. Else it will write the file    
![image](https://github.com/user-attachments/assets/0fb92d3a-81ca-43f1-be8d-0037492bf999)  
or  {both works}  
![image](https://github.com/user-attachments/assets/ca6e9dbf-8cdd-4cab-94e8-d07c4f7cde1c)  

#### Writing file in parquet format
![image](https://github.com/user-attachments/assets/bbabc3b5-40ef-4b6f-97c8-12e4c774a522)


#### DELTA format

Delta format is buit on top of parquet format  
In case of parquest , meta data is stored on the footer of the file. In Delta file format {ie delta lake}, metadata is stored in delta log, which is the transaction log file.{ not that the file format will remain as parquet  
#### Writing as table instead of file  
![image](https://github.com/user-attachments/assets/5ce9cacd-b92e-43a1-afc0-6addff5a9734)

#### MANAGED Vs EXTERNAL TABLES

MANAGED - when we create a table, the data that used used to create the table is managed by Databricks. So when we delete a table, the data is deleted as well.  
Extrenal tables - when we create a table on top od a df, the data will be stored in our own location, when we delete a table, the data will not be deleted{only the schema is deleted}. We create external tables by mentioning the corresponding location while creating the table. 
#### SPARK SQL Basic
sql remains same on spark. there is no performance difference between pyspark or sparksql.  
We read the data as a dataframe. To use sql, we need to convert the dataframe to an object {temporary view using createTempView function, which will be eliminated when the session ends} on which we can apply sql.  
![image](https://github.com/user-attachments/assets/91a7bd6d-ed20-4e89-a0bf-b2cae73b426c)
Now, to convert this view back to df to write, enter the query you were writing inside spark.sql as below  
![image](https://github.com/user-attachments/assets/cb340f57-0616-4bc6-b4f6-1903b85635c1)












