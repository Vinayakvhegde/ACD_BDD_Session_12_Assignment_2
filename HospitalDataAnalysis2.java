import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class HospitalDataAnalysis2 {
	public static void main(String[] args) throws Exception 
	{
		Configuration conf1 = new Configuration();
		if (args.length != 2) 
		{
			System.err.println("Usage: stdsubscriber <in> <out>");
			System.exit(2);
		}
		Job job1 = new Job(conf1, "Assignment 2 :Job1");
		job1.setJarByClass(HospitalDataAnalysis2.class);
		
		job1.setMapperClass(TokenizerMapper.class);
		job1.setMapOutputValueClass(IntWritable.class);
		
		job1.setReducerClass(SumReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		
		Path out1 = new Path(args[1]) ;
		out1.getFileSystem(conf1).delete(out1);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, out1);
		System.exit(job1.waitForCompletion(true) ? 0 : 1);
	}

	public static class SumReducer 
		extends	Reducer<Text, IntWritable, LongWritable, Text> {
		private LongWritable result = new LongWritable();
		public void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, LongWritable, Text>.Context context)
						throws IOException, InterruptedException {
			long  count = 0 ;
			for (IntWritable val : values) {
				count += val.get() ;
			}
			this.result.set(count);
			context.write(this.result, key);			
		}
	}

	public static class TokenizerMapper 
		extends	Mapper<Object, Text, Text, IntWritable> 
	{
		private final static IntWritable ONE = new IntWritable(1);
		Text question = new Text();
		public void map(Object key, Text value,	Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException 
		{
			if (value.toString() != null)
			{
				List<String> parts = CDRConstants.parseLine(value.toString());
				
				question.set(parts.get(CDRConstants.question));
				if (!(question.toString().isEmpty()))
					{
					context.write(question, ONE );
					}
			}
		}
	}
		
}

