package com.se4450.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.se4450.shared.HBaseUtils;

public class WriteToHbase {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		// format of hdfs line
		// sensorId-timestamp value

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String tokenArray[] = value.toString().split(" ");

			if (tokenArray.length < 2)
				return;

			String mapKey = tokenArray[0];
			String mapValue = tokenArray[1];
			
			//Emit key value pair
			context.write(new Text(mapKey), new Text(mapValue));
		}
	}

	public static class Reduce extends TableReducer<Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
		
			// KEY List(VALUES)
			// iterate through values and write to table
			for (Text value : values) {

				Put put = new Put(Bytes.toBytes(key.toString()));
				put.add(Bytes.toBytes("d"), Bytes.toBytes("val"),
						Bytes.toBytes(value.toString()));

				//Write to HBase
				context.write(key, put);
			}
		}
	}

	// Driver program
	public static void main(String[] args) throws Exception {

		// Hbase configuration
		Configuration conf = HBaseConfiguration.create();

		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs(); // get all args

		if (otherArgs.length != 2) {
			System.err.println("Usage: Write to HBase <in> <hbase-site.xml>");
			System.exit(2);
		}

		// Load in key,value pairs
		java.util.Map<String, Object> hbConf = new HashMap<String, Object>();
		if (HBaseUtils.LoadHBaseSiteData(args[1], hbConf)) {
			for (Entry<String, Object> value : hbConf.entrySet()) {
				conf.set(value.getKey(), value.getValue().toString());
			}
		}
		// create a job with name "Write to HBase "
		Job job = new Job(conf, "write to hbase");
		job.setJarByClass(WriteToHbase.class);
		job.setMapperClass(Map.class);

		// set output key type for mapper
		job.setMapOutputKeyClass(Text.class);
		// set output value type for mapper
		job.setMapOutputValueClass(Text.class);

		job.setNumReduceTasks(3);

		// set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));

		// connects to Hbase table SensorValues
		TableMapReduceUtil.initTableReducerJob("SensorValuesServingLayer",
				Reduce.class, job);

		job.setReducerClass(Reduce.class);

		// Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
