package com.lttltm.hadoop.kpi.querycount.fm;

import java.io.IOException;
import java.util.ResourceBundle;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import redis.clients.jedis.Jedis;

public class JedisOutputFormat extends OutputFormat<Text, IntWritable>{
	
	private static final  String REDIS_SERVER_IP;
	
	static
	{
		ResourceBundle bundle = ResourceBundle.getBundle("application");
		REDIS_SERVER_IP = bundle.getString("redis.server.ip");
	}

	@Override
	public RecordWriter<Text, IntWritable> getRecordWriter(
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		System.out.println("getRecordWriter");
		return new JedisRecordWriter(new Jedis(REDIS_SERVER_IP));
	}

	@Override
	public void checkOutputSpecs(JobContext context) throws IOException,
			InterruptedException {
		
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new FileOutputCommitter(FileOutputFormat.getOutputPath(context),
                context);
	}

}
