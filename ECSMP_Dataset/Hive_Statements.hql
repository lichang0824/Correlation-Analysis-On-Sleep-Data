# 1. create an external table called ERQ
  # id sets as integer
  # complete sets as String
  # expressive_suppression sets as Float
  # cognitive_reappraisal sets as Float
  # Skip the last row since it is a header
CREATE EXTERNAL TABLE ERQ (id INT, complete STRING, expressive_suppression FLOAT, cognitive_reappraisal FLOAT) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
LOCATION '/user/ytk244/final_project/hive_table/ERQ' 
tblproperties('skip.footer.line.count'='1');
describe ERQ;

# 2. create an external table called POMS
CREATE EXTERNAL TABLE POMS (id INT, complete STRING, POMS FLOAT) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
LOCATION '/user/ytk244/final_project/hive_table/POMS' 
tblproperties('skip.footer.line.count'='1');
describe POMS;

# 3. create an external table called SDS
CREATE EXTERNAL TABLE SDS (id INT, complete STRING, SDS FLOAT) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
LOCATION '/user/ytk244/final_project/hive_table/SDS' 
tblproperties('skip.footer.line.count'='1');
describe SDS;

# 4. create an external table called PSQI
CREATE EXTERNAL TABLE PSQI (id INT, complete STRING, PSQI FLOAT) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
LOCATION '/user/ytk244/final_project/hive_table/PSQI' 
tblproperties('skip.footer.line.count'='1');
describe PSQI;

# 5. create an external table called sleep_quality
CREATE EXTERNAL TABLE sleep_quality (id INT, sex STRING, age INT, ECG_Complete STRING, signal_quality STRING, time_in_bed FLOAT, sleep_duration FLOAT, deep_sleep FLOAT, light_sleep FLOAT, REM FLOAT, wake FLOAT, deep_sleep_onset FLOAT, sleep_efficiency FLOAT) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
LOCATION '/user/ytk244/final_project/hive_table/sleep_quality' 
tblproperties('skip.footer.line.count'='1');
describe sleep_quality;

# 6. create a table called merge_table
# select the needed data from POMS, SDS, PSQI, and sleep_quality tables 
# drop the unneeded columns
# and merge by user id
CREATE TABLE merge_table AS SELECT /*+ MAPJOIN(PSQI,POMS,SDS,ERQ) */  sleep_quality.*, 
PSQI.complete as PSQI_complete, PSQI.PSQI, POMS.complete as POMS_complete, POMS.POMS, 
ERQ.complete as ERQ_complete, ERQ.expressive_suppression, ERQ.cognitive_reappraisal, 
SDS.complete as SDS_complete, SDS.SDS FROM sleep_quality JOIN PSQI ON (sleep_quality.id = PSQI.id) 
JOIN POMS ON (POMS.id = PSQI.id) JOIN ERQ ON (ERQ.id = POMS.id) JOIN SDS ON (SDS.id = ERQ.id);
describe merge_table;

# 7. create a table called new_drop_and_clean_table
# that is used to store data after dropping unqualified or unfinished user rows
# the data will be stored in hdfs
CREATE TABLE new_drop_and_clean_table 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
LOCATION '/user/ytk244/final_project/hive_table/new_merge_drop' 
AS SELECT *  FROM merge_table WHERE ecg_complete = "Finished" and signal_quality = "qualified" 
and psqi_complete = "Finished" and poms_complete="Finished" and erq_complete="Finished" and sds_complete="Finished";
describe new_drop_and_clean_table;

# 8. create an external table called newCleanMergeDropTable
# this table is used to store data after MapReduce
# The MapReduce code will clean the merge data that is 
# get from new_drop_and_clean_table hive table.
# MapReduce code will drop the unused columns (Finished and qualified)
CREATE EXTERNAL TABLE newCleanMergeDropTable (id INT, sex STRING, age INT, time_in_bed FLOAT, sleep_duration FLOAT, deep_sleep FLOAT, light_sleep FLOAT, REM FLOAT, wake FLOAT, deep_sleep_onset FLOAT, sleep_efficiency FLOAT, psqi FLOAT, poms FLOAT, expressive_suppression FLOAT, cognitive_reappraisal FLOAT, sds FLOAT) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
LOCATION '/user/ytk244/final_project/hive_table/newCleanMergeDrop' 
tblproperties('skip.footer.line.count'='1');
describe newCleanMergeDropTable;

# 9. create a table called col_avg
# This table is used to store each column's average value
CREATE TABLE col_avg ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
LOCATION '/user/ytk244/final_project/hive_table/col_avg' 
AS SELECT AVG(age) as avg_age, AVG(time_in_bed) as avg_time_in_bed, 
AVG(sleep_duration) as avg_sleep_duration, AVG(deep_sleep) as avg_deep_sleep, 
AVG(light_sleep) as avg_light_sleep, AVG(REM) as avg_rem, AVG(wake) as avg_wake, 
AVG(deep_sleep_onset) as avg_deep_sleep_onset, AVG(sleep_efficiency) as avg_sleep_efficiency, 
AVG(psqi) as avg_psqi, AVG(poms) as avg_poms, AVG(expressive_suppression) as avg_expressive_suppression, 
AVG(cognitive_reappraisal) as avg_cognitive_reappraisal, AVG(sds) as avg_sds 
FROM newCleanMergeDropTable;
describe col_avg;

# 10. create a table called col_min
# This table is used to store each column's minimum value
CREATE TABLE col_min ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
LOCATION '/user/ytk244/final_project/hive_table/col_min' 
AS SELECT MIN(age) as min_age, MIN(time_in_bed) as min_time_in_bed, 
MIN(sleep_duration) as min_sleep_duration, MIN(deep_sleep) as min_deep_sleep, 
MIN(light_sleep) as min_light_sleep, MIN(REM) as min_rem, MIN(wake) as min_wake, 
MIN(deep_sleep_onset) as min_deep_sleep_onset, MIN(sleep_efficiency) as min_sleep_efficiency, 
MIN(psqi) as min_psqi, MIN(poms) as min_poms, MIN(expressive_suppression) as min_expressive_suppression, 
MIN(cognitive_reappraisal) as min_cognitive_reappraisal, MIN(sds) as min_sds 
FROM newCleanMergeDropTable;
describe col_min;

# 11. create a table called col_max
# This table is used to store each column's maximum value
CREATE TABLE col_max ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
LOCATION '/user/ytk244/final_project/hive_table/col_max' 
AS SELECT MAX(age) as max_age, MAX(time_in_bed) as max_time_in_bed, 
MAX(sleep_duration) as max_sleep_duration, MAX(deep_sleep) as max_deep_sleep, 
MAX(light_sleep) as max_light_sleep, MAX(REM) as max_rem, MAX(wake) as max_wake, 
MAX(deep_sleep_onset) as max_deep_sleep_onset, MAX(sleep_efficiency) as max_sleep_efficiency, 
MAX(psqi) as max_psqi, MAX(poms) as max_poms, MAX(expressive_suppression) as max_expressive_suppression, 
MAX(cognitive_reappraisal) as max_cognitive_reappraisal, MAX(sds) as max_sds 
FROM newCleanMergeDropTable;
describe col_max;

# 12. create a table called time_in_bed_corr
# This table is used to find the correlations between time_in_bed variable and other questionnaires' variables
CREATE TABLE time_in_bed_corr ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
LOCATION '/user/ytk244/final_project/hive_table/time_in_bed_corr' 
AS SELECT 
corr(time_in_bed,psqi) as time_in_bed_psqi,
corr(time_in_bed,poms) as time_in_bed_poms,
corr(time_in_bed,expressive_suppression) as time_in_bed_expressive_suppression,
corr(time_in_bed,cognitive_reappraisal) as time_in_bed_cognitive_reappraisal,
corr(time_in_bed,sds) as time_in_bed_sds 
FROM newCleanMergeDropTable;
describe time_in_bed_corr;

# 13. create a table called sleep_efficiency_corr
# This table is used to find the correlations between sleep_efficiency variable and other questionnaires' variables
CREATE TABLE sleep_efficiency_corr ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
LOCATION '/user/ytk244/final_project/hive_table/sleep_efficiency_corr' 
AS SELECT 
corr(sleep_efficiency,psqi) as psqi,
corr(sleep_efficiency,poms) as poms,
corr(sleep_efficiency,expressive_suppression) as expressive_suppression,
corr(sleep_efficiency,cognitive_reappraisal) as cognitive_reappraisal,
corr(sleep_efficiency,sds) as sds 
FROM newCleanMergeDropTable;
describe sleep_efficiency_corr;

# 14. create a table called sleep_duration_corr
# This table is used to find the correlations between sleep_duration variable and other questionnaires' variables
CREATE TABLE sleep_duration_corr ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
LOCATION '/user/ytk244/final_project/hive_table/sleep_duration_corr' 
AS SELECT 
corr(sleep_duration,psqi) as psqi,
corr(sleep_duration,poms) as poms,
corr(sleep_duration,expressive_suppression) as expressive_suppression,
corr(sleep_duration,cognitive_reappraisal) as cognitive_reappraisal,
corr(sleep_duration,sds) as sds 
FROM newCleanMergeDropTable;
describe sleep_duration_corr;
select * from sleep_duration_corr;

# 15. create a table called deep_sleep_corr
# This table is used to find the correlations between deep_sleep variable and other questionnaires' variables
CREATE TABLE deep_sleep_corr ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
LOCATION '/user/ytk244/final_project/hive_table/deep_sleep_corr' 
AS SELECT 
corr(deep_sleep,psqi) as psqi,
corr(deep_sleep,poms) as poms,
corr(deep_sleep,expressive_suppression) as expressive_suppression,
corr(deep_sleep,cognitive_reappraisal) as cognitive_reappraisal,
corr(deep_sleep,sds) as sds 
FROM newCleanMergeDropTable;
describe deep_sleep_corr;
select * from deep_sleep_corr;

# 16. create a table called light_sleep_corr
# This table is used to find the correlations between light_sleep variable and other questionnaires' variables
CREATE TABLE light_sleep_corr ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
LOCATION '/user/ytk244/final_project/hive_table/light_sleep_corr' 
AS SELECT 
corr(light_sleep,psqi) as psqi,
corr(light_sleep,poms) as poms,
corr(light_sleep,expressive_suppression) as expressive_suppression,
corr(light_sleep,cognitive_reappraisal) as cognitive_reappraisal,
corr(light_sleep,sds) as sds 
FROM newCleanMergeDropTable;
describe light_sleep_corr;
select * from light_sleep_corr;

# 17. create a table called rem_corr
# This table is used to find the correlations between rem variable and other questionnaires' variables
CREATE TABLE rem_corr ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
LOCATION '/user/ytk244/final_project/hive_table/rem_corr' 
AS SELECT 
corr(rem,psqi) as psqi,
corr(rem,poms) as poms,
corr(rem,expressive_suppression) as expressive_suppression,
corr(rem,cognitive_reappraisal) as cognitive_reappraisal,
corr(rem,sds) as sds 
FROM newCleanMergeDropTable;
describe rem_corr;
select * from rem_corr;

# 18. create a table called poms_corr
# This table is used to find the correlations between poms variable and questionnaires variables
CREATE TABLE poms_corr ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
LOCATION '/user/ytk244/final_project/hive_table/poms_corr' 
AS SELECT 
corr(poms,psqi) as psqi,
corr(poms,sds) as sds,
corr(poms,expressive_suppression) as expressive_suppression,
corr(poms,cognitive_reappraisal) as cognitive_reappraisal 
FROM newCleanMergeDropTable;
describe poms_corr;
select * from poms_corr;

# 19. create a table called psqi_corr
# This table is used to find the correlations between psqi variable and questionnaires variables
CREATE TABLE psqi_corr ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
LOCATION '/user/ytk244/final_project/hive_table/psqi_corr' 
AS SELECT 
corr(psqi,poms) as poms,
corr(psqi,sds) as sds,
corr(psqi,expressive_suppression) as expressive_suppression,
corr(psqi,cognitive_reappraisal) as cognitive_reappraisal 
FROM newCleanMergeDropTable;
describe psqi_corr;
select * from psqi_corr;

# 20. create a table called sds_corr
# This table is used to find the correlations between sds variable and questionnaires variables
CREATE TABLE sds_corr ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
LOCATION '/user/ytk244/final_project/hive_table/sds_corr' 
AS SELECT 
corr(sds,psqi) as psqi,
corr(sds,poms) as poms,
corr(sds,expressive_suppression) as expressive_suppression,
corr(sds,cognitive_reappraisal) as cognitive_reappraisal 
FROM newCleanMergeDropTable;
describe sds_corr;
select * from sds_corr;

# 21. create a table called expressive_suppression_corr
# This table is used to find the correlations between expressive_suppression variable and questionnaires variables
CREATE TABLE expressive_suppression_corr ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
LOCATION '/user/ytk244/final_project/hive_table/expressive_suppression_corr' 
AS SELECT 
corr(expressive_suppression,psqi) as psqi,
corr(expressive_suppression,sds) as sds,
corr(poms,expressive_suppression) as poms,
corr(expressive_suppression,cognitive_reappraisal) as cognitive_reappraisal 
FROM newCleanMergeDropTable;
describe expressive_suppression_corr;
select * from expressive_suppression_corr;

# 22. create a table called cognitive_reappraisal_corr
# This table is used to find the correlations between cognitive_reappraisal variable and questionnaires variables
CREATE TABLE cognitive_reappraisal_corr ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
LOCATION '/user/ytk244/final_project/hive_table/cognitive_reappraisal_corr' 
AS SELECT 
corr(cognitive_reappraisal,psqi) as psqi,
corr(cognitive_reappraisal,sds) as sds,
corr(poms,cognitive_reappraisal) as poms,
corr(expressive_suppression,cognitive_reappraisal) as expressive_suppression 
FROM newCleanMergeDropTable;
describe cognitive_reappraisal_corr;
select * from cognitive_reappraisal_corr;