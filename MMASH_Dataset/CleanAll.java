import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class CleanAll {
	
	public static void main(String[] args) throws Exception {
		// create jobs
		Job actigraph = new Job();
		Job activity = new Job();
		Job rr = new Job();
		Job questionnaire = new Job();
		Job info = new Job();
		Job sleep = new Job();
		
		// set Key-Value seperator
		// got this from https://stackoverflow.com/questions/16614029/hadoop-output-key-value-separator
		// and https://www.tabnine.com/code/java/methods/org.apache.hadoop.mapreduce.Job/getConfiguration
		actigraph.getConfiguration().set(TextOutputFormat.SEPERATOR, ",");
		activity.getConfiguration().set(TextOutputFormat.SEPERATOR, ",");
		rr.getConfiguration().set(TextOutputFormat.SEPERATOR, ",");
		questionnaire.getConfiguration().set(TextOutputFormat.SEPERATOR, ",");
		info.getConfiguration().set(TextOutputFormat.SEPERATOR, ",");
		sleep.getConfiguration().set(TextOutputFormat.SEPERATOR, ",");
		
		// set jar
		actigraph.setJarByClass(CleanAll.class);
		activity.setJarByClass(CleanAll.class);
		rr.setJarByClass(CleanAll.class);
		questionnaire.setJarByClass(CleanAll.class);
		info.setJarByClass(CleanAll.class);
		sleep.setJarByClass(CleanAll.class);
		
		// set name
		actigraph.setJobName("Actigraph");
		activity.setJobName("Activity");
		rr.setJobName("RR");
		questionnaire.setJobName("Questionnaire");
		info.setJobName("Info");
		sleep.setJobName("Sleep");
		
		// set in path
		for (int i = 1; i <= 22; i++) {
			if (i == 11) continue; // user_11 does not have data in sleep.csv, skipping
			if (i == 13) continue; // user_13 has missing data in questionnaire.csv, skipping
			if (i == 18) continue; // user_18 has an age of 0, skipping
			String user = Integer.toString(i);
			String actigraphIn = "project_data/DataPaper/user_" + user + "/Actigraph.csv";
			String activityIn = "project_data/DataPaper/user_" + user + "/Activity.csv";
			String rrIn = "project_data/DataPaper/user_" + user + "/RR.csv";
			String questionnaireIn = "project_data/DataPaper/user_" + user + "/questionnaire.csv";
			String infoIn = "project_data/DataPaper/user_" + user + "/user_info.csv";
			String sleepIn = "project_data/DataPaper/user_" + user + "/sleep.csv";
			FileInputFormat.addInputPath(actigraph, new Path(actigraphIn));
			FileInputFormat.addInputPath(activity, new Path(activityIn));
			FileInputFormat.addInputPath(rr, new Path(rrIn));
			FileInputFormat.addInputPath(questionnaire, new Path(questionnaireIn));
			FileInputFormat.addInputPath(info, new Path(infoIn));
			FileInputFormat.addInputPath(sleep, new Path(sleepIn));
		}
		
		// set out path
		String actigraphOut = "project_data_cleaned/Actigraph";
		String activityOut = "project_data_cleaned/Activity";
		String rrOut = "project_data_cleaned/RR";
		String questionnaireOut = "project_data_cleaned/Questionnaire";
		String infoOut = "project_data_cleaned/Info";
		String sleepOut = "project_data_cleaned/Sleep";
		FileOutputFormat.setOutputPath(actigraph, new Path(actigraphOut));
		FileOutputFormat.setOutputPath(activity, new Path(activityOut));
		FileOutputFormat.setOutputPath(rr, new Path(rrOut));
		FileOutputFormat.setOutputPath(questionnaire, new Path(questionnaireOut));
		FileOutputFormat.setOutputPath(info, new Path(infoOut));
		FileOutputFormat.setOutputPath(sleep, new Path(sleepOut));
		
		// set mapper classes
		actigraph.setMapperClass(ActigraphMapper.class);
		activity.setMapperClass(ActivityMapper.class);
		rr.setMapperClass(RRMapper.class);
		questionnaire.setMapperClass(QuestionnaireMapper.class);
		info.setMapperClass(InfoMapper.class);
		sleep.setMapperClass(SleepMapper.class);
		
		// set key and value type
		actigraph.setOutputKeyClass(IntWritable.class);
		activity.setOutputKeyClass(IntWritable.class);
		rr.setOutputKeyClass(IntWritable.class);
		questionnaire.setOutputKeyClass(IntWritable.class);
		info.setOutputKeyClass(IntWritable.class);
		sleep.setOutputKeyClass(IntWritable.class);
		actigraph.setOutputValueClass(Text.class);
		activity.setOutputValueClass(Text.class);
		rr.setOutputValueClass(Text.class);
		questionnaire.setOutputValueClass(Text.class);
		info.setOutputValueClass(Text.class);
		sleep.setOutputValueClass(Text.class);
		
		// set number of reduce tasks
		actigraph.setNumReduceTasks(0);
		activity.setNumReduceTasks(0);
		rr.setNumReduceTasks(0);
		questionnaire.setNumReduceTasks(0);
		info.setNumReduceTasks(0);
		sleep.setNumReduceTasks(0);
		
		// wait for completion
		actigraph.waitForCompletion(true);
		activity.waitForCompletion(true);
		rr.waitForCompletion(true);
		questionnaire.waitForCompletion(true);
		info.waitForCompletion(true);
		sleep.waitForCompletion(true);
	}
}
