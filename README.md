# Correlation-Analysis-On-Sleep-Data
## Motivation
  This study is important for the general public because sleep is such a vital part of our life. Sleep not only affects our mood and emotions, but it can also tangibly and meaningfully affect our productivity and professional performance. Previous research has shown that sleep deprivation can impact our neurological performance, such as our ability to remember and think.   
  As college students, this kind of ability is especially important. In addition to that, college students can have less regular sleep patterns and more variability in their sleep, since they need to cope with so many aspects of their life, all the while maintaining academic performance. Therefore, this study is not only important for the general public to understand the relationship between what we do on a daily basis and our sleep quality, therefore our emotions and neurological performance. 

## Tech used
**Built with** 
- Hadoop Ecosystem
  - PySpark
  - Hive
  - Hadoop MapReduce
  - HDFS
- pandas
- matplotlib

## Code for Dataset 1 - MMASH
The MMASH dataset: Multilevel Monitoring of Activity and Sleep in Healthy people [1],[2],[3] can be downloaded by the following link: https://physionet.org/content/mmash/1.0.0/

### All code is highly automated. It would run seamlessly, and it can be run multiple times without breaking (idempotent). 
### However, all code and data must be present in the correct places, and shells must be run in a specific order. 
### Use [doall.sh](/MMASH_Dataset/doall.sh) to execute all code except the Pandas Analysis. Alternatively, run the commands in [doall.sh](/MMASH_Dataset/doall.sh) one at a time.

### Hive queries are stored as a file and run as non-interactive script. A passwordfile is necessary. The passwordfile in the repository does NOT contain password. Change netID and edit passwordfile.txt if necessary. Alternatively, copy code from ```.hql``` files into Hive and execute. 

Before Running, my home directory looks like this. It should contain all code. 
![Home_Files.png](/MMASH_Dataset/Screenshots/Home_Files.png)
Use [compile.sh](/MMASH_Dataset/compile.sh) to recompile if necessary. Recompiling will remove old .class files. 

Before Running, HDFS should look like this. The folder "project_data" should contain project data (the DataPaper folder). 
![HDFS_Files.png](/MMASH_Dataset/Screenshots/HDFS_Files.png)

If all code and data are put in the places described above, the code will run. However, it can also be changed if necessary. 
Giving input and output paths via arguments is time-consuming and messy. Hardcoding improves automation. 

### Step 1 - Upload Data to HDFS
![1_Upload_Data_to_Peel.png](/MMASH_Dataset/Screenshots/1_Upload_Data_to_Peel.png)
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
![4_distinct_state.png](/MMASH_Dataset/Screenshots/4_distinct_state.png)

select distinct gender from info;\
![4_distinct_gender.png](/MMASH_Dataset/Screenshots/4_distinct_gender.png)

select max(ibi_s) from rr;\
![4_max_ibi_s.png](/MMASH_Dataset/Screenshots/4_max_ibi_s.png)

select min(ibi_s) from rr;\
![4_min_ibi_s.png](/MMASH_Dataset/Screenshots/4_min_ibi_s.png)

describe questionnaire;\
![4_describe_questionnaire.png](/MMASH_Dataset/Screenshots/4_describe_questionnaire.png)

describe sleep;\
![4_describe_sleep.png](/MMASH_Dataset/Screenshots/4_describe_sleep.png)

select distinct activity_type from activity;\
![4_distinct_activity_type.png](/MMASH_Dataset/Screenshots/4_distinct_activity_type.png)

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
![7_Pandas_Import_Clean_Merge.png](/MMASH_Dataset/Screenshots/7_Pandas_Import_Clean_Merge.png)

Show all columns.\
![7_Pandas_Show_All_Columns.png](/MMASH_Dataset/Screenshots/7_Pandas_Show_All_Columns.png)

Show strong positive correlations.\
![7_Pandas_Show_Positive_Correlations.png](/MMASH_Dataset/Screenshots/7_Pandas_Show_Positive_Correlations.png)

Show strong negative correlations.\
![7_Pandas_Show_Negative_Correlations.png](/MMASH_Dataset/Screenshots/7_Pandas_Show_Negative_Correlations.png)

Show scatterplot with best fit line.\
![7_Pandas_Show_Two_Best_Fit_Lines.png](/MMASH_Dataset/Screenshots/7_Pandas_Show_Two_Best_Fit_Lines.png)

## Code for Dataset 2 - ECSMP: A dataset on Emotion, cognition, sleep, and multi-model physiological signals

The ECSMP dataset: A dataset on emotion, cognition, sleep, and multi-model physiological signals [4], [5]can be downloaded by the following link: https://data.mendeley.com/datasets/vn5nknh3mn/2

sleep quality.xlsx contains each participant's sleep quality analysis result. Each row represents a test subject

scale.xlsx contains multiple sheets and each sheet represents a questionnaire. Each row in each questionnaire represents a test subject. The questionnaires that are used in this project are:
- The Emotion Regulation Questionnaire (ERQ)
- The Self-Rating Depression Scale (SDS)
- The Profile of Mood States Questionnaire (POMS) - 40 items
- The Pittsburgh Sleep Quality Index (PSQI)

### Step 1 - Upload Datasets to HDFS

### Step 2 - Transfer datasets with `.xlsx` format to `.csv` format
Since the datasets are in .xlsx format, the first step is to transfer the datasets in .xlsx format to .csv format using PySpark and pandas.
The codes to follow can be found in the folder ECSMP_Dataset/Change_Datasets_Format. The folder includes five .py files, which are Sleep_Quality.py, ERQ.py, POMS.py, PSQI.py, and SDS.py.
The way to run the codes is to copy and paste them directly into the PySpark Interactive Shell.

### Step 3 - Data Profiling
Using MapReduce written in Java to profile the ERQ, POMS, PSQI, SDS, and Sleep_Quality csv datasets by counting the test subject records inside the dataset. The MapReduce profiling codes are stored in ECSMP_Dataset/Data_Profiling.

### Step 4 - Data Cleaning
Using MapReduce written in Java to clean the ERQ, POMS, PSQI, SDS, and Sleep_Quality csv datasets. Each dataset has a corresponding MapReduce cleaning code. The codes are stored in the folders with the following naming "Clean_[Dataset Name]" (ex: Clean_POMS). Then store all the clean outputs obtained from Reducer to HDFS.

### Step 5 - Data Aggregation
Using the Clean datasets from the previous step to create the Hive tables. Each dataset will have a corresponding Hive table. In the end, these Hive tables will be merged into one Hive table, called merge_table.

Run `Beeline` to access Hive Grunt Shell. Copy and Paste the Hive statements directly into the Hive Grunt Shell to create Hive tables.

#### 5.1 Create the Hive tables: 
Copy and paste the first six Hive statements stored in ECSMP_Dataset/Hive_Statements.hql to create the Hive tables corresponding to ERQ, POMS, PSQI, SDS, and Sleep Quality datasets and the merge Hive table.

#### 5.2 Drop the "Unfinished" or "Unqualified" rows: 
Use the seventh Hive statement stored in ECSMP_Dataset/Hive_Statements.hql to create a Hive internal table called new_drop_and_clean_table that is used to store the data after dropping "Unfinished" or "Unqualified" data from the merge_table. 
The data is defined as "Unfinished" or "Unqualified" based on the "Complete" column in ERQ, POMS, PSQI, and SDS datasets and the "ECG Complete" column and "Signal Quality" column in the Sleep Quality dataset.

#### 5.3 The data included in the new_drop_and_clean_table is then stored in HDFS

### Step 6 - Data Profiling
Profile the data get from the new_drop_and_clean_table Hive table, using the same MapReduce profiling codes that are stored in ECSMP_Dataset/Data_Profiling.

### Step 7 - Data Cleaning
Using MapReduce written in Java to clean the data get from the Hive table, new_drop_and_clean_table. The code is stored in ECSMP_Dataset/Clean_Merge. Then store the clean output from Reducer to HDFS. This output data is used to do the final analysis. Let's call it "Final_Dataset."

### Step 8 - Data Analysis
#### 8.1 Using Hive tables
Use the eighth Hive statement in ECSMP_Dataset/Hive_Statements.hql to create a Hive external table called newCleanMergeDropTable. This Hive table includes the Final_Dataset. Use this newCleanMergeDropTable Hive table to do further analysis in Hive.

Using the Hive statements starting at the 9th statement in ECSMP_Dataset/Hive_Statements.hql to create multiple internal Hive tables that store different analyses.

Hive Tables Created: (the "other variables" mentioned below are PSQI, POMS, SDS, Expressive Suppression, and Cognitive Reappraisal variables)

- col_avg: stores each column's average value.
- col_min: stores each column's minimum value.
- col_max: stores each column's maximum value.
- time_in_bed_corr: stores the correlation between - time_in_bed and other variables
- sleep_efficiency_corr: stores the correlation - between sleep_efficiency and other variables
- sleep_duration_corr: stores the correlation between sleep_duration and other variables
- deep_sleep_corr: stores the correlation between - deep_sleep and other variables
- light_sleep_corr: stores the correlation between light_sleep and other variables
- rem_corr: stores the correlation between rem and other variables
- poms_corr: stores the correlation between poms and other variables
- psqi_corr: stores the correlation between psqi and other variables
- sds_corr: stores the correlation between sds and other variables
- expressive_suppression_corr: stores the correlation between expressive_suppression and other variables
- cognitive_reappraisal_corr: stores the correlation between cognitive_reappraisal and other variables

#### 8.2 Using pandas and matplotlib
Get Final_Dataset from HDFS to Home directory. Then, use `scp` to download the Final_Dataset to local machine for pandas. Use the code stored in ECSMP_Dataset/Correlation.ipynb to generate the Correlation Matrix that shows all the correlations among all variables included in the Final_Dataset.

## References
[1] A. Goldberger, L. Amaral, L. Glass, J. Hausdorff, P.C. Ivanov, R. Mark, J.E. Mietus, G.B. Moody, C.K. Peng, and H.E. Stanley, “PhysioBank, PhysioToolkit, and PhysioNet: Components of a new research resource for complex physiologic signals,” 2000. [Online]. 101 (23), pp. e215–e220. [Accessed: 05-May-2022].   
[2] A. Rossi, E. D. Pozzo, D. Menicagli, C. Tremolanti, C. Priami, A. Sirbu, D. Clifton, C. Martini, and D. Morelli, “Multilevel monitoring of activity and sleep in healthy people,” Multilevel Monitoring of Activity and Sleep in Healthy People v1.0.0, 19-Jun-2020. [Online]. Available: https://physionet.org/content/mmash/1.0.0/. [Accessed: 06-May-2022].  
[3] A. Rossi, E. Da Pozzo, D. Menicagli, C. Tremolanti, C. Priami, A. Sîrbu, D. A. Clifton, C. Martini, and D. Morelli, “A public dataset of 24-H multi-levels psycho-physiological responses in young healthy adults,” MDPI, 25-Sep-2020. [Online]. Available: https://www.mdpi.com/2306-5729/5/4/91. [Accessed: 06-May-2022].  
[4] Z. Gao, X. Cui, W. Wan, W. Zheng, and Z. Gu, “ECSMP: A dataset on emotion, cognition, sleep, and multi-model physiological signals,” Data in Brief, 01-Dec-2021. [Online]. Available: https://www.sciencedirect.com/science/article/pii/S2352340921009355. [Accessed: 06-May-2022].  
[5] Z. Gao, X. Cui, wang wan, W. Zheng, and Z. Gu, “ECSMP: A dataset on emotion, cognition, sleep, and multi-model physiological signals,” Mendeley Data, 04-Nov-2021. [Online]. Available: https://data.mendeley.com/datasets/vn5nknh3mn/2. [Accessed: 06-May-2022].   