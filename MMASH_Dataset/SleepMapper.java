import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.hadoop.mapreduce.Mapper;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;

import java.util.ArrayList;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class SleepMapper
	extends Mapper<LongWritable, Text, IntWritable, Text> {
	
	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
		String line = value.toString();
		String[] list = line.split(",");
		
		LocalTime in, out;
		int day;
		double latency, total_minutes_in_bed, total_sleep_time, wake_after_sleep_onset, num_awakening, fragmentation_index, efficiency, avg_awakening_length, movement_index, sleep_fragmentation_index;
		
		if (list[0].equals("")) return; // skip header
		try {
			day = Integer.parseInt(list[1]);
			in = LocalTime.parse(list[2], DateTimeFormatter.ofPattern("HH:mm"));
			out = LocalTime.parse(list[4], DateTimeFormatter.ofPattern("HH:mm"));
			latency = Double.parseDouble(list[7]);
			efficiency = Double.parseDouble(list[8]);
			total_minutes_in_bed = Double.parseDouble(list[9]);
			total_sleep_time = Double.parseDouble(list[10]);
			wake_after_sleep_onset = Double.parseDouble(list[11]);
			num_awakening = Double.parseDouble(list[12]);
			avg_awakening_length = Double.parseDouble(list[13]);
			movement_index = Double.parseDouble(list[14]);
			fragmentation_index = Double.parseDouble(list[15]);
			sleep_fragmentation_index = Double.parseDouble(list[16]);
		}
		catch (Exception e) {
			context.write(new IntWritable(-1), new Text(e.getMessage()));
			return;
		}

		// got this from https://stackoverflow.com/questions/19012482/how-to-get-the-input-file-name-in-the-mapper-in-a-hadoop-program
		FileSplit fileSplit = (FileSplit)context.getInputSplit();
		String filepath = fileSplit.getPath().toString();
		
		// inspired by https://stackoverflow.com/questions/2367381/how-to-extract-numbers-from-a-string-and-get-an-array-of-ints
		Pattern p = Pattern.compile("user_-?\\d+");
		Matcher m = p.matcher(filepath);
		String username = "";
		while (m.find()) {
			username = m.group();
		}
		p = Pattern.compile("\\d+");
		m = p.matcher(username);
		int user = 0;
		while (m.find()) {
			user = Integer.parseInt(m.group());
		}
		
		ArrayList<String> cleanList = new ArrayList<String>();
		int inDay = day;
		int outDay = day;
		// got the compareTo function from https://www.geeksforgeeks.org/localtime-compareto-method-in-java-with-examples/
		if (out.compareTo(in) < 0) {
			inDay = 1;
			outDay = 2;
		}
		cleanList.add("2000-01-0" + Integer.toString(inDay) + " " + in.toString() + ":00");
		cleanList.add("2000-01-0" + Integer.toString(outDay) + " " + out.toString() + ":00");
		cleanList.add(Double.toString(latency));
		cleanList.add(Double.toString(efficiency));
		cleanList.add(Double.toString(total_minutes_in_bed));
		cleanList.add(Double.toString(total_sleep_time));
		cleanList.add(Double.toString(wake_after_sleep_onset));
		cleanList.add(Double.toString(num_awakening));
		cleanList.add(Double.toString(avg_awakening_length));
		cleanList.add(Double.toString(movement_index));
		cleanList.add(Double.toString(fragmentation_index));
		cleanList.add(Double.toString(sleep_fragmentation_index));
		
		context.write(new IntWritable(user), new Text(this.makeLine(cleanList)));
	}
	
	public static String makeLine(ArrayList<String> list) {
		String r = "";
		for (String str : list) {
			r += str + ",";
		}
		r = r.substring(0, r.length() - 1);
		return r;
	}

}
