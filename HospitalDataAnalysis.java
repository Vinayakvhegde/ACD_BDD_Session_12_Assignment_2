import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class HospitalDataAnalysis {
	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		if (args.length != 2) 
		{
			System.err.println("Usage: stdsubscriber <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "Assignment 2 :NYC Hospital data");
		job.setJarByClass(HospitalDataAnalysis.class);
		job.setMapperClass(TokenizerMapper.class);
		//job.setCombinerClass(SumReducer.class);
		job.setReducerClass(SumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		
		Path out = new Path(args[1]) ;
		out.getFileSystem(conf).delete(out);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class SumReducer 
		extends	Reducer<Text, FloatWritable, Text, FloatWritable> {
		private FloatWritable result = new FloatWritable();
	
		public void reduce(Text key, Iterable<FloatWritable> values,
				Reducer<Text, FloatWritable, Text, FloatWritable>.Context context)
						throws IOException, InterruptedException {
			float sumPercent = 0;
			long  count = 0 ;
			for (FloatWritable val : values) {
				sumPercent += (val.get()/100);
				count += 1 ;
			}
			this.result.set((sumPercent/count)*100);
			context.write(key, this.result);
		}
	}

	public static class TokenizerMapper 
		extends	Mapper<Object, Text, Text, FloatWritable> 
	{
		Text hospital = new Text();
		FloatWritable ansPercent = new FloatWritable();
		public void map(Object key, Text value,	Mapper<Object, Text, Text, FloatWritable>.Context context)
				throws IOException, InterruptedException 
		{
			List<String> parts = CDRConstants.parseLine(value.toString());
			hospital.set(parts.get(CDRConstants.hospitalName));
			ansPercent.set(Float.parseFloat(parts.get(CDRConstants.answerPercent)));
			context.write(hospital, ansPercent);
		}
	}
		
}