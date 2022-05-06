import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;

import java.util.ArrayList;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class RRMapper
        extends Mapper<LongWritable, Text, IntWritable, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
                String line = value.toString();
                String[] list = line.split(",");
                
		int id, day;
		double ibi_s;
		String time;
		LocalTime localTime;
		
		if (list[0].equals("")) return; // skip header
		try {
			id = Integer.parseInt(list[0]);
			ibi_s = Double.parseDouble(list[1]);
			day = Integer.parseInt(list[2]);
			time = list[3];
			localTime = LocalTime.parse(time, DateTimeFormatter.ofPattern("HH:mm:ss"));
			if (day < 1) throw new Exception();
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
		cleanList.add(Double.toString(ibi_s));
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
