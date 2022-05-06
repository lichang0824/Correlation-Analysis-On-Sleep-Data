hdfs dfs -rm -r -f project_tables/*
beeline -u jdbc:hive2://hm-1.hpc.nyu.edu:10000/cl5034 -n cl5034 -w /home/cl5034/passwordfile.txt -f /home/cl5034/hive_query.hql
