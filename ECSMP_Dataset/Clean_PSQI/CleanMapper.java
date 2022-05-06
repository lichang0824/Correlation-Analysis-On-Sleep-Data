import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanMapper extends Mapper<LongWritable, Text, Text, Text> { // Clean PSQI_Table
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1); // This splitting regex idea is learned from the article, Regex: Splitting by Character, Unless in Quotes. The following is the article link: https://stackabuse.com/regex-splitting-by-character-unless-in-quotes/ 
        int sleep_qua, sleep_lat, sleep_dur, sleep_eff, sleep_dis, hypnotic, daytime_dys, PSQIScore;
        if(key.get() == 0) { // if is header
            context.write(new Text("ID,Complete,PSQI"), new Text(""));
        } else if(line.length == 32) {
            String userId = line[0];
            String complete = line[3];
            try {
              sleep_qua = Integer.parseInt(line[24]);
              sleep_lat = Integer.parseInt(line[25]);
              sleep_dur = Integer.parseInt(line[26]);
              sleep_eff = Integer.parseInt(line[27]);
              sleep_dis = Integer.parseInt(line[28]);
              hypnotic = Integer.parseInt(line[29]);
              daytime_dys = Integer.parseInt(line[30]);
              PSQIScore = sleep_qua+ sleep_lat+ sleep_dur+ sleep_eff+ sleep_dis+ hypnotic+ daytime_dys;
            } catch(Exception e) {
              return;
            }
            
            if(isInt(line[0])) { // if is user ID 
                context.write(new Text(userId + "," + complete + "," + PSQIScore), new Text(""));
            }
        }
    }
    
    public static boolean isInt(String input) {
        try {
            return true;
        }
        catch(Exception e) {
            return false;
        }
    }
}


