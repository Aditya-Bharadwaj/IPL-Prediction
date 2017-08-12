import java.io.IOException;
import java.util.StringTokenizer;
import java.lang.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PVPMapper extends Mapper<LongWritable, Text, Text, Text>
{
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		/*
			0,      1,     2, 3, 4, 5, 6, 7, 8,        9,   10,   11
			Batsman,Bowler,0s,1s,2s,3s,4s,6s,Dismissal,Runs,Balls,SR
		*/
		if(key.get() != 0)
		{
			String line = value.toString();
			String[] vals = line.split(",");
			String batsmanName = vals[0];
			String bowlerName = vals[1];
			String pair = batsmanName + "," + bowlerName;
			String balls = vals[2] + "," + vals[3] + "," + vals[4] + "," + vals[5] + "," + vals[6] + "," + vals[7] + "," + vals[8] + "," + vals[9] + "," + vals[10] + "," + vals[11];
			context.write(new Text(pair), new Text(balls));
			//(V Kohli,P Kumar	{'2,3,4,5,6,7,8,9,10,11',...}
		}
	}
}
