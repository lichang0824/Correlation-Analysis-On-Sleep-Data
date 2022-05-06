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

public class ActigraphMapper
	extends Mapper<LongWritable, Text, IntWritable, Text> {
	
	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
		String line = value.toString();
		String[] list = line.split(",");
		
		int id, steps, off, standing, sitting, lying, day;
		double hr;
		String time;
		LocalTime localTime;
		
		if (list[0].equals("")) return; // skip header
		try {
			id = Integer.parseInt(list[0]);
			steps = Integer.parseInt(list[4]);
			hr = Double.parseDouble(list[5]);
			off = Integer.parseInt(list[6]);
			standing = Integer.parseInt(list[7]);
			sitting = Integer.parseInt(list[8]);
			lying = Integer.parseInt(list[9]);
			day = Integer.parseInt(list[11]);
			time = list[12];
			localTime = LocalTime.parse(time, DateTimeFormatter.ofPattern("HH:mm:ss"));
			// got this from https://stackoverflow.com/questions/11994790/parse-time-of-format-hhmmss#comment15996893_11994790
			if (steps < 0) throw new Exception();
			if (hr < 0.0) throw new Exception();
			if (off != 0 && off != 1) throw new Exception();
			if (standing != 0 && standing != 1) throw new Exception();
			if (sitting != 0 && sitting != 1) throw new Exception();
			if (lying != 0 && lying != 1) throw new Exception();
			if (off + standing + sitting + lying != 1) throw new Exception();
			if (day != 1 && day != 2) throw new Exception();
		}
		catch (Exception e) {
			return; // skip lines that have error
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
		cleanList.add(Integer.toString(steps));
		cleanList.add(Double.toString(hr));
		if (off == 1) cleanList.add("off");
		if (standing == 1) cleanList.add("standing");
		if (sitting == 1) cleanList.add("sitting");
		if (lying == 1) cleanList.add("lying");
		//cleanList.add("2000-01-0" + Integer.toString(day) + " " + localTime.toString());
	
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
