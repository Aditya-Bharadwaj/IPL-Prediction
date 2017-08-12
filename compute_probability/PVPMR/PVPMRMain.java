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

public class PVPMRMain
{
	public static void main(String[] args) throws Exception
	{
		Job run1 = Job.getInstance();
		run1.setJarByClass(PVPMRMain.class);
		run1.setJobName("PvP Map Reduce");
		
		FileInputFormat.addInputPath(run1, new Path(args[0]));
		FileOutputFormat.setOutputPath(run1, new Path(args[1]));

		run1.setMapperClass(PVPMapper.class);
		run1.setReducerClass(PVPReducer.class);

		run1.setOutputKeyClass(Text.class);
		run1.setOutputValueClass(Text.class);
		
	   	System.exit(run1.waitForCompletion(true) ? 0 : 1);

	}
}
