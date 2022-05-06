rm *.class
rm cleanall.jar
hdfs dfs -rm -r -f project_data_cleaned/*
javac -classpath `yarn classpath` -d . ActigraphMapper.java
javac -classpath `yarn classpath` -d . ActivityMapper.java
javac -classpath `yarn classpath` -d . RRMapper.java
javac -classpath `yarn classpath` -d . QuestionnaireMapper.java
javac -classpath `yarn classpath` -d . InfoMapper.java
javac -classpath `yarn classpath` -d . SleepMapper.java
javac -classpath `yarn classpath`:. -d . CleanAll.java
jar -cvf cleanall.jar *.class
hadoop jar cleanall.jar CleanAll
