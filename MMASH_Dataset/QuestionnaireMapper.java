import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.hadoop.mapreduce.Mapper;

import java.util.ArrayList;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class QuestionnaireMapper
	extends Mapper<LongWritable, Text, IntWritable, Text> {
	
	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
		String line = value.toString();
		String[] list = line.split(",");
		
		int meq, stai1, stai2, pittsburgh, daily_stress, bisbas_bis, bisbas_reward, bisbas_drive, bisbas_fun, panas_pos_10, panas_pos_14, panas_pos_18, panas_pos_22, panas_pos_9p1, panas_neg_10, panas_neg_14, panas_neg_18, panas_neg_22, panas_neg_9p1;
		
		if (list[0].equals("")) return; // skip header
		try {
			meq = Integer.parseInt(list[1]);
			stai1 = Integer.parseInt(list[2]);
			stai2 = Integer.parseInt(list[3]);
			pittsburgh = Integer.parseInt(list[4]);
			daily_stress = Integer.parseInt(list[5]);
			bisbas_bis = Integer.parseInt(list[6]);
			bisbas_reward = Integer.parseInt(list[7]);
			bisbas_drive = Integer.parseInt(list[8]);
			bisbas_fun = Integer.parseInt(list[9]);
			panas_pos_10 = Integer.parseInt(list[10]);
			panas_pos_14 = Integer.parseInt(list[11]);
			panas_pos_18 = Integer.parseInt(list[12]);
			panas_pos_22 = Integer.parseInt(list[13]);
			panas_pos_9p1 = Integer.parseInt(list[14]);
			panas_neg_10 = Integer.parseInt(list[15]);
			panas_neg_14 = Integer.parseInt(list[16]);
			panas_neg_18 = Integer.parseInt(list[17]);
			panas_neg_22 = Integer.parseInt(list[18]);
			panas_neg_9p1 = Integer.parseInt(list[19]);
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
		cleanList.add(Integer.toString(meq));
		cleanList.add(Integer.toString(stai1));
		cleanList.add(Integer.toString(stai2));
		cleanList.add(Integer.toString(pittsburgh));
		cleanList.add(Integer.toString(daily_stress));
		cleanList.add(Integer.toString(bisbas_bis));
		cleanList.add(Integer.toString(bisbas_reward));
		cleanList.add(Integer.toString(bisbas_drive));
		cleanList.add(Integer.toString(bisbas_fun));
		cleanList.add(Integer.toString(panas_pos_10));
		cleanList.add(Integer.toString(panas_pos_14));
		cleanList.add(Integer.toString(panas_pos_18));
		cleanList.add(Integer.toString(panas_pos_22));
		cleanList.add(Integer.toString(panas_pos_9p1));
		cleanList.add(Integer.toString(panas_neg_10));
		cleanList.add(Integer.toString(panas_neg_14));
		cleanList.add(Integer.toString(panas_neg_18));
		cleanList.add(Integer.toString(panas_neg_22));
		cleanList.add(Integer.toString(panas_neg_9p1));
		
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
