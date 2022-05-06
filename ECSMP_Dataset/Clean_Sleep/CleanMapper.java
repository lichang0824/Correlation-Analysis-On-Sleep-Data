import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanMapper extends Mapper<LongWritable, Text, Text, Text> { // Clean Sleep_Table
  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] line = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)",-1); // This splitting regex idea is learned from the article, Regex: Splitting by Character, Unless in Quotes. The following is the article link: https://stackabuse.com/regex-splitting-by-character-unless-in-quotes/ 
    if(key.get() == 0) { // if is header
        context.write(new Text("userId,sex,age,ecg_complete,signal_quality,time_in_bed,sleep_duration,deep_sleep,light_sleep,rem,wake,deep_sleep_onset,sleep_efficiency"), new Text(""));
    } else if(line.length == 19) {
        String userId = line[0];
        String sex = line[1];
        String age = line[2];
        String ecg_complete = line[3];
        String signal_quality = line[6];
        String time_in_bed = line[8];
        String sleep_duration = line[9];
        String deep_sleep = line[10];
        String light_sleep = line[11];
        String rem = line[12];
        String wake = line[13];
        String deep_sleep_onset = line[14];
        String sleep_efficiency = line[15];

        if(isInt(line[0])) { // if is user ID 
          context.write(new Text(userId + "," + sex + "," + age + "," + ecg_complete + ","
          + signal_quality+ "," + time_in_bed + "," + sleep_duration+ "," + deep_sleep + "," + light_sleep +
          "," + rem + "," + wake + "," + deep_sleep_onset + "," + sleep_efficiency), new Text(""));
        }
    }
  }
  
  public static boolean isInt(String input) {
    try {
        Integer.parseInt(input);
        return true;
    }
    catch(Exception e) {
        return false;
    }
  }
}
