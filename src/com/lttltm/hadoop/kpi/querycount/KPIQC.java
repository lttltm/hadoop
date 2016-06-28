package com.lttltm.hadoop.kpi.querycount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.lttltm.hadoop.kpi.KPI;
import com.lttltm.hadoop.kpi.querycount.fm.JedisOutputFormat;
import com.lttltm.hadoop.qq.QQSecondRelationship;

public class KPIQC {
	
	public static class KPIQCMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{

		private final static IntWritable one = new IntWritable(1);
		 private Text word = new Text();
		 
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			KPI kip = KPI.filterPVs(value.toString());
			
			if(kip.getRequest_url().contains("query?query=")){
				String[] strs = kip.getRequest_url().split("query=");
				word.set(strs[1]);
				context.write(word, one);
			}
		}
		
		
	}
	
	
	public static class KPIQCReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
		
		
	}

	public static void main(String[] args) throws Exception {
		Path in = new Path("/usr/test/kpi/input/");
		
		System.setProperty("hadoop.home.dir","D:/hadoop-2.7.2");
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "KPIPV");
		
		job.setJarByClass(QQSecondRelationship.class);
		String osName = System.getProperty("os.name");
	    if (osName.startsWith("Windows")) {
	    	job.setJar("C:/Users/Administrator/Desktop/hadoop.jar");
	    }
		
		job.setMapperClass(KPIQCMapper.class);
		job.setCombinerClass(KPIQCReducer.class);
		job.setReducerClass(KPIQCReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, in);
		
		job.setOutputFormatClass(JedisOutputFormat.class);
		
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
