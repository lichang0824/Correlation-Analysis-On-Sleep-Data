import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanMapper extends Mapper<LongWritable, Text, Text, Text> { // Clean Merge_Table
  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] line = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)",-1); // This splitting regex idea is learned from the article, Regex: Splitting by Character, Unless in Quotes. The following is the article link: https://stackabuse.com/regex-splitting-by-character-unless-in-quotes/ 
    if(key.get() == 0) { // Just use as a condition to write down the header
      context.write(new Text("userId,sex,age,time_in_bed,sleep_duration,deep_sleep,light_sleep,rem,wake,deep_sleep_onset,sleep_efficiency,psqi,poms,expressive_suppression,cognitive_reappraisal,sds"), new Text(""));
    }
    if(line.length == 22) { // the data needs to include 22 columns
      /**
       * Double checked the data status is finished and qualified, although data was filtered and checked already in Hive           
       */
      String userId = line[0];       
      String sex = line[1];
      String age = line[2];
      String ecg_complete = line[3];
      if(!ecg_complete.equals("Finished")) {
        return;
      }
      String signal_quality = line[4];
      if(!signal_quality.equals("qualified")) {
        return;
      }
      String time_in_bed = line[5];
      String sleep_duration = line[6];
      String deep_sleep = line[7];
      String light_sleep = line[8];
      String rem = line[9];
      String wake = line[10];
      String deep_sleep_onset = line[11];
      String sleep_efficiency = line[12];
      String psqi_complete = line[13];
      if(!psqi_complete.equals("Finished")) {
        return;
      }
      String psqi = line[14];
      String poms_complete = line[15];
      if(!poms_complete.equals("Finished")) {
        return;
      }
      String poms = line[16];
      String erq_complete = line[17];
      if(!erq_complete.equals("Finished")) {
        return;
      }
      String expressive_suppression = line[18];
      String cognitive_reappraisal = line[19];
      String sds_complete = line[20];
      if(!sds_complete.equals("Finished")) {
        return;
      }
      String sds = line[21];

      /**
       *  Check if all the data types are correct, if correct
       *  Clean the data by dropping the unused columns in data analysis part
       *  Drop status since I already checked they are all finished and qualified
       *                             
       */
      if(isInt(userId) && isInt(age) && isFloat(time_in_bed) && isFloat(sleep_duration) && isFloat(deep_sleep) 
      && isFloat(light_sleep) && isFloat(rem) && isFloat(wake) && isFloat(deep_sleep_onset) && isFloat(sleep_efficiency) 
      && isFloat(psqi) && isFloat(poms) && isFloat(expressive_suppression)&& isFloat(cognitive_reappraisal) && isFloat(sds)) {
        context.write(new Text(userId + "," + sex + "," + age + "," + time_in_bed + "," + sleep_duration+ "," + deep_sleep + "," 
        + light_sleep + "," + rem + "," + wake + "," + deep_sleep_onset + "," + sleep_efficiency + "," + 
        psqi + "," + poms + "," + expressive_suppression + "," + cognitive_reappraisal + "," + sds), new Text(""));
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

  public static boolean isFloat(String input) {
    try {
        Float.parseFloat(input);
        return true;
    }
    catch(Exception e) {
        return false;
    }
  }
}

