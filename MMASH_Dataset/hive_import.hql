use cl5034;

drop table if exists actigraph;
create table actigraph (username int, steps int, hr double, state string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;
load data inpath 'hdfs://horton.hpc.nyu.edu:8020/user/cl5034/project_data_cleaned/Actigraph' into table actigraph;

drop table if exists activity;
create table activity (username int, activity_type int, start_time timestamp, end_time timestamp) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;
load data inpath 'hdfs://horton.hpc.nyu.edu:8020/user/cl5034/project_data_cleaned/Activity' into table activity;

drop table if exists rr;
create table rr (username int, ibi_s double) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;
load data inpath 'hdfs://horton.hpc.nyu.edu:8020/user/cl5034/project_data_cleaned/RR' into table rr;

drop table if exists info;
create table info (username int, gender string, weight int, height int, age int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;
load data inpath 'hdfs://horton.hpc.nyu.edu:8020/user/cl5034/project_data_cleaned/Info' into table info;

drop table if exists questionnaire;
create table questionnaire (username int, meq double, stai1 double, stai2 double, pittsburgh double, daily_stress double, bisbas_bis double, bisbas_reward double, bisbas_drive double, bisbas_fun double, panas_pos_10 double, panas_pos_14 double, panas_pos_18 double, panas_pos_22 double, panas_pos_9p1 double, panas_neg_10 double, panas_neg_14 double, panas_neg_18 double, panas_neg_22 double, panas_neg_9p1 double) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;
load data inpath 'hdfs://horton.hpc.nyu.edu:8020/user/cl5034/project_data_cleaned/Questionnaire' into table questionnaire;

drop table if exists sleep;
create table sleep (username int, in_time timestamp, out_time timestamp, latency double, efficiency double, total_minutes_in_bed double, total_sleep_time double, wake_after_sleep_onset double, num_awakening double, avg_awakening_length double, movement_index double, fragmentation_index double, sleep_fragmentation_index double) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;
load data inpath 'hdfs://horton.hpc.nyu.edu:8020/user/cl5034/project_data_cleaned/Sleep' into table sleep;
