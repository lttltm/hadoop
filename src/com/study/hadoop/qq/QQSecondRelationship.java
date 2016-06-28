package com.study.hadoop.qq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/*
hadoop	cat
dog	cat
hello	world
world	earth
cat	fash
hadoop	hdfs
hdfs	mapreduce



 */
public class QQSecondRelationship {
	
	private static Logger log = LoggerFactory.getLogger(QQSecondRelationship.class);
	
	public static class QQSecondRelationshipMapper extends Mapper<LongWritable, Text, Text, Text>
	{

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			log.info("≥ı ºªØ.....");
			super.setup(context);
		}

		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			
			log.info("map.....");
			String relationStr = value.toString();
			
			if(relationStr != null && !relationStr.trim().isEmpty())
			{
				String[] friends = relationStr.split("\t");
				
				context.write(new Text(friends[0]), new Text(friends[1]));
				context.write(new Text(friends[1]), new Text(friends[0]));
			}
			
		}
		
	}
	
	
	public static class QQSecondRelationshipReducer extends Reducer<Text, Text, Text, Text>{

		@Override
		protected void reduce(Text text, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			List<String> friends = new ArrayList<String>();
			
			
			values.forEach(value -> friends.add(value.toString()));
			
			friends.forEach(f1 ->{
				friends.forEach(f2 ->{
					if(!f1.equals(f2))
					{
						try {
							System.out.println(f1+"----------"+f2);
							context.write(new Text(f1), new Text(f2));
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				});
			});
			
//			for(Text value : values)
//			{
//				friends.add(value.toString());
//			}
//			friends.forEach(f -> System.out.println("friends----------"+f));
//			
//			for (int i=0 ; i< friends.size() ; i++)
//			{
//				for(int j=i+1 ; j< friends.size() ; j++)
//				{
//					System.out.println(friends.get(i)+":"+friends.get(j));
//					context.write(friends.get(i), friends.get(j));
//				}
//			}
		}
		
	}

	public static void main(String[] args) throws Exception {
		
		Path in = new Path("/usr/test/qq/input/friends.txt");
//		Path in = new Path("data/friends.txt");
		Path out = new Path("/usr/test/qq/output/");
		
		System.setProperty("hadoop.home.dir","D:/hadoop-2.7.2");
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "qq");
//		conf.set("mapreduce.jobtracker.address", "local");
		
		job.setJarByClass(QQSecondRelationship.class);
		String osName = System.getProperty("os.name");
	    if (osName.startsWith("Windows")) {
	    	job.setJar("C:/Users/Administrator/Desktop/hadoop.jar");
	    }
		
		job.setMapperClass(QQSecondRelationshipMapper.class);
		job.setReducerClass(QQSecondRelationshipReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
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
