package com.study.hadoop.kpi.pv;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.study.hadoop.kpi.KPI;
import com.study.hadoop.qq.QQSecondRelationship;
import com.study.hadoop.qq.QQSecondRelationship.QQSecondRelationshipMapper;
import com.study.hadoop.qq.QQSecondRelationship.QQSecondRelationshipReducer;

public class KPIPV {

	public static class KPIPVMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		private IntWritable one = new IntWritable(1);
		private Text url = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();

			KPI kpi = KPI.filterPVs(line);

			if(kpi.isValid())
			{
				url.set(kpi.getRequest_url());
				context.write(url, one);
			}
		}
		
	}
	
	public static class KPIPVReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		 private IntWritable result = new IntWritable();

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			
			int sum = 0;
			
			for(IntWritable value : values)
			{
				sum += value.get();
			}
			result.set(sum);
			context.write(key, result);
		}

		
	}
	
	public static void main(String[] args) throws Exception {
		Path in = new Path("/usr/test/kpi/input/");
//		Path in = new Path("data/friends.txt");
		Path out = new Path("/usr/test/kpi/output/");
		
		System.setProperty("hadoop.home.dir","D:/hadoop-2.7.2");
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "KPIPV");
		
		job.setJarByClass(QQSecondRelationship.class);
		String osName = System.getProperty("os.name");
	    if (osName.startsWith("Windows")) {
	    	job.setJar("C:/Users/Administrator/Desktop/hadoop.jar");
	    }
		
		job.setMapperClass(KPIPVMapper.class);
		job.setReducerClass(KPIPVReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		FileSystem fs = FileSystem.get(conf);
		
		if(fs.exists(out))
		{
			fs.delete(out,true);
		}
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
