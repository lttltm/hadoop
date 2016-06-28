package com.lttltm.hadoop.kpi.querycount.fm;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import redis.clients.jedis.Jedis;

public class JedisRecordWriter extends RecordWriter<Text, IntWritable> {
	
	private Jedis jedis;
	
	

	public JedisRecordWriter(Jedis jedis) {
		this.jedis = jedis;
	}

	@Override
	public void write(Text key, IntWritable value) throws IOException,
			InterruptedException {
		
		if(key == null || value == null)
		{
			return;
		}
		
		String keyStr = key.toString();
		int score = value.get();
		
		for(int i=0 ,size = keyStr.length();i<size; i++){
			System.out.println("zadd " + keyStr.substring(0, i+1) +" " + score + " " +  keyStr);
			jedis.zadd(keyStr.substring(0, i+1), score, keyStr);
		}

	}

	@Override
	public void close(TaskAttemptContext context) throws IOException,
			InterruptedException {
		jedis = null;
	}

}
