## Code for Dataset 1 - MMASH

### All code is highly automated. It would run seamlessly, and it can be run multiple times without breaking (idempotent). 
### However, all code and data must be present in the correct places, and shells must be run in a specific order. 
### Use [doall.sh](/MMASH_Dataset/doall.sh) to execute all code except the Pandas Analysis. Alternatively, run the commands in [doall.sh](/MMASH_Dataset/doall.sh) one at a time.

### Hive queries are stored as a file and run as non-interactive script. A passwordfile is necessary. The passwordfile in the repository does NOT contain password. Change netID and edit passwordfile.txt if necessary. Alternatively, copy code from ```.hql``` files into Hive and execute. 

Before Running, my home directory looks like this. It should contain all code. 
![Home_Files.png](/Screenshots/Home_Files.png)
Use [compile.sh](/MMASH_Dataset/compile.sh) to recompile if necessary. Recompiling will remove old .class files. 

Before Running, HDFS should look like this. The folder "project_data" should contain project data (the DataPaper folder). 
![HDFS_Files.png](/Screenshots/HDFS_Files.png)

If all code and data are put in the places described above, the code will run. However, it can also be changed if necessary. 
Giving input and output paths via arguments is time-consuming and messy. Hardcoding improves automation. 

### Step 1 - Upload Data to HDFS
![1_Upload_Data_to_Peel.png](/Screenshots/1_Upload_Data_to_Peel.png)
After uploading, copy this data from home directory to hdfs directory. 
``` hdfs dfs -put DataPaper project_data/```

### Step 2 - Clean Data Using MapReduce
Use [clean.sh](/MMASH_Dataset/clean.sh) to launch the cleaning MR job. 
6 jobs will be launched. Just wait for completion. 
The input and output paths are hardcoded in [CleanAll.java](/MMASH_Dataset/CleanAll.java). 
The cleaned results from mappers will be stored in project_data_cleaned. 

### Step 3 - Import Cleaned Table into Hive
Use [hive_import.sh](/MMASH_Dataset/hive_import.sh) to copy data from last step into Hive.
This will create 6 tables in hive. 

### Step 4 - Hive Profiling
Use [hive_profile.sh](/MMASH_Dataset/hive_profile.sh) to run profiling queries in Hive.
This will print the output to the shell, and not write any tables. 
Here're the outputs. 

select distinct state from actigraph;\
![4_distinct_state.png](/Screenshots/4_distinct_state.png)

select distinct gender from info;\
![4_distinct_gender.png](/Screenshots/4_distinct_gender.png)

select max(ibi_s) from rr;\
![4_max_ibi_s.png](/Screenshots/4_max_ibi_s.png)

select min(ibi_s) from rr;\
![4_min_ibi_s.png](/Screenshots/4_min_ibi_s.png)

describe questionnaire;\
![4_describe_questionnaire.png](/Screenshots/4_describe_questionnaire.png)

describe sleep;\
![4_describe_sleep.png](/Screenshots/4_describe_sleep.png)

select distinct activity_type from activity;\
![4_distinct_activity_type.png](/Screenshots/4_distinct_activity_type.png)

### Step 5 - Hive Aggregation
Use [hive_query.sh](/MMASH_Dataset/hive_query.sh) to aggregate tables and generate output. 
This will create 6 tables in project_tables in HDFS. 
5 tables will be aggregated to 19 rows. Activity needs to be aggregated in Pandas. It is only partly aggregated here. 

### Step 6 - Fetch Tables for Pandas
Use [gettable.sh](/MMASH_Dataset/gettable.sh) to get table from HDFS to Home directory. 
Then, use ```scp``` to download these tables to local machine for Pandas. 

### Step 7 - Pandas Correlation
Open [Correlation_Analysis.ipynb](/MMASH_Dataset/Pandas_Correlation/Correlation_Analysis.ipynb) with jupyterlab, and run the file. 
This notebook will import tables from the previous step. Import paths can be adjusted as necessary. For convenience, it is included. 

This notebook will further aggregate the activity table, filling in with 0 if the user did not do the corresponding activity.
Then, it will do the correlation analysis. High correlation pairs are printed and two scatterplots with best fit line are drawn. 

Import, clean, and merge.\
![7_Pandas_Import_Clean_Merge.png](/Screenshots/7_Pandas_Import_Clean_Merge.png)

Show all columns.\
![7_Pandas_Show_All_Columns.png](/Screenshots/7_Pandas_Show_All_Columns.png)

Show strong positive correlations.\
![7_Pandas_Show_Positive_Correlations.png](/Screenshots/7_Pandas_Show_Positive_Correlations.png)

Show strong negative correlations.\
![7_Pandas_Show_Negative_Correlations.png](/Screenshots/7_Pandas_Show_Negative_Correlations.png)

Show scatterplot with best fit line.\
![7_Pandas_Show_Two_Best_Fit_Lines.png](/Screenshots/7_Pandas_Show_Two_Best_Fit_Lines.png)
