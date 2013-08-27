package org.notmysock.orc;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.filecache.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.reduce.*;

import org.apache.commons.cli.*;
import org.apache.commons.*;

import java.nio.*;
import java.util.*;
import java.net.*;
import java.math.*;
import java.security.*;
import java.util.concurrent.*;

import java.io.IOException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.*;
import org.apache.hadoop.hive.ql.io.orc.*;
import org.apache.hadoop.hive.ql.io.orc.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;

public class ORCery extends Configured implements Tool {
	
	private static class SampleRow {
		int L_ORDERKEY;
		int L_PARTKEY;		
		int L_SUPPKEY;
		int L_LINENUMBER;
		double L_QUANTITY;
		double L_EXTENDEDPRICE;
		double L_DISCOUNT;
		double L_TAX;
		String L_RETURNFLAG;
		String L_LINESTATUS;
		String L_COMMITDATE;
		String L_RECEIPTDATE;
		String L_SHIPINSTRUCT;
		String L_SHIPMODE;
		String L_COMMENT;
		
		static int row = 0;
		
		public SampleRow() {
			L_ORDERKEY = ++row;
			L_PARTKEY = row % 7;
			L_SUPPKEY = row % 11;
			L_LINENUMBER = row % 5;
			L_QUANTITY = 1.0 * L_LINENUMBER;
			L_EXTENDEDPRICE = 1.0 * L_LINENUMBER;
			L_DISCOUNT = 0.0;
			L_TAX = 0.1 * L_EXTENDEDPRICE;
			L_RETURNFLAG = (row % 23 == 0) ? "yes" : null ;
			L_LINESTATUS = "shipped";
			L_COMMITDATE = "28-08-2013";
			L_RECEIPTDATE = null;
			L_SHIPINSTRUCT = "fragile";
			L_SHIPMODE = "air";
			L_COMMENT = null;
		}
	}
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new ORCery(), args);
        System.exit(res);
    }
    
    @Override
    public int run(String[] args) throws Exception {
    	Configuration conf = getConf();
		String[] remainingArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		CommandLineParser parser = new BasicParser();
		org.apache.commons.cli.Options options = new org.apache.commons.cli.Options();
		options.addOption("o", "output", true, "output");
		options.addOption("n", "number", true, "number");

		CommandLine line = parser.parse(options, remainingArgs);

		if (!(line.hasOption("output"))) {
			HelpFormatter f = new HelpFormatter();
			f.printHelp("ORCery", options);
			return 1;
		}
		
		Path out = new Path(line.getOptionValue("output"));
		int n = 2;
		
		if(line.hasOption('n')) {
			n = Integer.parseInt(line.getOptionValue('n'));
		}
		
		HiveConf.setVar(conf,HiveConf.ConfVars.HIVE_ORC_WRITE_FORMAT, "0.12"); // use latest format
		FileSystem fs = FileSystem.get(conf);
		int stripeSize = 16*1024*1024;
		CompressionKind compress = CompressionKind.ZLIB;
		int bufferSize = 262144; //OrcFile.DEFAULT_COMPRESSION_BLOCK_SIZE
		int rowIndexStride = 0;
		ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(SampleRow.class, ObjectInspectorOptions.JAVA);
		Writer writers[] = new Writer[n];
		for (int i = 0 ; i < n; i++) {
			Path path = new Path(out, String.format("%03d", i));
			writers[i] = OrcFile.createWriter(fs, path, conf, inspector, stripeSize, compress, bufferSize, rowIndexStride);
			fs.deleteOnExit(path);
		}
		
		for (int i = 0; i < n; i++) {
			writers[i].addRow(new SampleRow());
		}
		
		for (int i = 0; i < n; i++) {
			writers[i].close();
		}
		fs.deleteOnExit(out);
		
	    return 0;
    }
}
