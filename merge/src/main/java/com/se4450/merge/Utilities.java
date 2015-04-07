package com.se4450.merge;

import org.apache.hadoop.conf.Configuration;

public class Utilities {
	
	public static Configuration loadHBaseConfiguration(Configuration config)
	{
		config.set("hbase.tmp.dir", "/home/hduser/hbase/tmp");
		config.set("hbase.rootdir", "hdfs://master/hbase");
		config.set("hbase.cluster.distributed", "true");
		config.set("hbase.local.dir", "/home/hduser/hbase/local");
		config.set("hbase.master.info.port", "6010");
		config.set("hbase.zookeeper.quorum", "zookeeper-1,zookeeper-2,zookeeper-3,");
		return config;
	}

}
