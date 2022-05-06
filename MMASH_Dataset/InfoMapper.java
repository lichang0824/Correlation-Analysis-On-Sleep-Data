import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.hadoop.mapreduce.Mapper;

import java.util.ArrayList;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class InfoMapper
	extends Mapper<LongWritable, Text, IntWritable, Text> {
	
	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
		String line = value.toString();
		String[] list = line.split(",");
		
		char sex;
		int weight, height, age;

		if (list[0].equals("")) return; // skip header
		try {
			if (list[1].length() != 1) throw new Exception();
			sex = list[1].charAt(0);
			weight = Integer.parseInt(list[2]);
			height = Integer.parseInt(list[3]);
			age = Integer.parseInt(list[4]);
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
		cleanList.add(Character.toString(sex));
		cleanList.add(Integer.toString(weight));
		cleanList.add(Integer.toString(height));
		cleanList.add(Integer.toString(age));
		
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
