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

public class GvGMapper extends Mapper<LongWritable, Text, Text, Text>
{
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		/*
			0,             1,	     2, 3, 4, 5, 6, 7, 8
			BatsmanCluster,BowlerCluster,0s,1s,2s,3s,4s,6s,Dismissal
		*/
		if(key.get() != 0)
		{
			String line = value.toString();
			String[] vals = line.split(",");
			String batClusterNum = vals[0];
			String bowlClusterNum = vals[1];
			String pair = batClusterNum + "," + bowlClusterNum;
			String probs = vals[2] + "," + vals[3] + "," + vals[4] + "," + vals[5] + "," + vals[6] + "," + vals[7] + "," + vals[8];
			context.write(new Text(pair), new Text(probs));
			//(V Kohli,P Kumar	{'2,3,4,5,6,7,8,9,10,11',...}
		}
	}
}
