import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanMapper extends Mapper<LongWritable, Text, Text, Text> { // Clean SDS_Table
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)",-1); // This splitting regex idea is learned from the article, Regex: Splitting by Character, Unless in Quotes. The following is the article link: https://stackabuse.com/regex-splitting-by-character-unless-in-quotes/ 
        if(key.get() == 0) { // if is header
          context.write(new Text("ID,Complete,SDS_Score"), new Text(""));
        } else if(line.length == 26) {
          String userId = line[0];
          String complete = line[3];
          String SDSScore = line[25];
          if(isInt(line[0])) { // if is user ID 
              context.write(new Text(userId + "," + complete + "," + SDSScore), new Text(""));
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
