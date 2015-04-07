package com.se4450.hadoop.test;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.se4450.hadoop.WriteToHbase.Map;

public class WriteToHbaseTest {
	MapDriver<LongWritable, Text, Text, Text> mapDriver;
	ReduceDriver<Text, Text, ImmutableBytesWritable, Mutation> reduceDriver;

	@Before
	public void setUp() {
		Map mapper = new Map();
		mapDriver = MapDriver.newMapDriver(mapper);
	}

	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new LongWritable(),
				new Text("1-12345679890123 100"));
		mapDriver.withOutput(new Text("1-12345679890123"), new Text("100"));
		mapDriver.runTest();
	}
	
	@Test
	public void testMapper_tooManyInputs() throws IOException {
		mapDriver.withInput(new LongWritable(),
				new Text("1-12345679890123 100 1"));
		mapDriver.withOutput(new Text("1-12345679890123"), new Text("100"));
		mapDriver.runTest();
	}
	
	@Test
	public void testMapper_notEnoughInputs() throws IOException {
		mapDriver.withInput(new LongWritable(), new Text("1-12345679890123"));

		mapDriver.runTest();

	}
}
