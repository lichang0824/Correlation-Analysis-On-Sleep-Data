rm *.class
rm cleanall.jar
javac -classpath `yarn classpath` -d . ActigraphMapper.java
javac -classpath `yarn classpath` -d . ActivityMapper.java
javac -classpath `yarn classpath` -d . RRMapper.java
javac -classpath `yarn classpath` -d . QuestionnaireMapper.java
javac -classpath `yarn classpath` -d . InfoMapper.java
javac -classpath `yarn classpath` -d . SleepMapper.java
javac -classpath `yarn classpath`:. -d . CleanAll.java
jar -cvf cleanall.jar *.class
