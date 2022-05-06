set hive.cli.print.header=true;

drop table if exists actigraph_agg;
create table actigraph_agg row format delimited fields terminated by ',' stored as textfile
location '/user/cl5034/project_tables/actigraph'
as select username, sum(steps) as total_steps, avg(hr) as average_heart_rate from actigraph group by username;

drop table if exists activity_agg;
create table activity_agg row format delimited fields terminated by ',' stored as textfile
location '/user/cl5034/project_tables/activity'
as select username, activity_type, sum((unix_timestamp(end_time) - unix_timestamp(start_time)) / 60) as time_diff from activity group by username, activity_type;

drop table if exists rr_agg;
create table rr_agg row format delimited fields terminated by ',' stored as textfile
location '/user/cl5034/project_tables/rr'
as select username, avg(ibi_s) as average_ibi_s from rr group by username;

drop table if exists sleep_agg;
create table sleep_agg row format delimited fields terminated by ',' stored as textfile
location '/user/cl5034/project_tables/sleep'
as select username, avg(latency) as latency, avg(efficiency) as efficiency, sum(total_minutes_in_bed) as total_minutes_in_bed, sum(total_sleep_time) as total_sleep_time, sum(wake_after_sleep_onset) as wake_after_sleep_onset, sum(num_awakening) as num_awakening, avg(avg_awakening_length) as avg_awakening_length, avg(movement_index) as movement_index, avg(fragmentation_index) as fragmentation_index, avg(sleep_fragmentation_index) as sleep_fragmentation_index from sleep group by username;

drop table if exists info_agg;
create table info_agg row format delimited fields terminated by ',' stored as textfile
location '/user/cl5034/project_tables/info'
as select * from info;

drop table if exists questionnaire_agg;
create table questionnaire_agg row format delimited fields terminated by ',' stored as textfile
location '/user/cl5034/project_tables/questionnaire'
as select * from questionnaire;
