package com.htuts.HBaseTutorials;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HBaseBulkLoader extends Configured implements Tool {
	
	private static final byte[] COL_FAM = Bytes.toBytes("f");
	
	private static final byte[] TAB_NAME= Bytes.toBytes("wordcount");
	

	public int run(String[] args) throws Exception {
		 
        // Obtain Table reference
        Configuration hbaseConf = HBaseConfiguration.create();
        //hbaseConf.writeXml(System.out);
        Connection connect = ConnectionFactory.createConnection(hbaseConf);
        Table table = null;

        if (!connect.getAdmin().isTableAvailable(TableName.valueOf(args[2]))) {
               System.out.println("Table " + args[2] + " does not exist. Create one before executing again");
               System.exit(1);
        }
        table = connect.getTable(TableName.valueOf(args[2]));

        Job job = Job.getInstance(hbaseConf, "HBase Loader");

        job.setJarByClass(HBaseBulkLoader.class);

        job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        job.setMapperClass(HBaseBulkLoaderMapper.class);
        
        RegionLocator rl = connect.getRegionLocator(TableName.valueOf(args[2]));
   	    HFileOutputFormat2.configureIncrementalLoad(job, table, rl);
               
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));     

        if (job.waitForCompletion(true)) {
               LoadIncrementalHFiles loader = new LoadIncrementalHFiles(hbaseConf);
               loader.doBulkLoad(new Path(args[1]), connect.getAdmin(), table, rl);
        } else {
               System.out.println("Loading Failed");
               return 1;
        }

        table.close();
        return 0;

 }
	public int runall(String[] args) throws Exception {
		
		//Obtain Table reference
		Configuration hbaseConf = HBaseConfiguration.create();
		//conf.set("hadoop.tmp.dir", args[1]);
		hbaseConf.set("hbase.fs.tmp.dir", args[1]);
		Connection connect = ConnectionFactory.createConnection(hbaseConf);
		Table table = null;
		
		for (String arg: args) 
			System.out.println("arguments are " + arg);
		
		if (!connect.getAdmin().isTableAvailable(TableName.valueOf(args[2]))) {
			System.out.println("Table " + args[2] + " does not exist. Create one before executing again");
			System.exit(1);
		}

		table = connect.getTable(TableName.valueOf(args[2]));
		RegionLocator rl = connect.getRegionLocator(TableName.valueOf(args[2]));


		// Configure Job for execution
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "HBase Loader");
		job.setJarByClass(HBaseBulkLoader.class);
		job.setMapperClass(HBaseBulkLoaderMapper.class);
		
		job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);
		job.setOutputFormatClass(HFileOutputFormat2.class);
		
		FileInputFormat.setInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
				
		System.out.println("region locator: "  + rl.getAllRegionLocations());

		HFileOutputFormat2.configureIncrementalLoad(job, table, rl);

		  Path outputpath = new Path(args[1]);
	        outputpath.getFileSystem(conf).delete(outputpath, true);

		if (job.waitForCompletion(true)) {
			LoadIncrementalHFiles loader = new LoadIncrementalHFiles(hbaseConf);
			loader.doBulkLoad(new Path(args[1]), connect.getAdmin(), table, rl);
		} else {
			System.out.println("Loading Failed");
			return 1;
		}
		
		table.close();
		connect.close();
		return 0;

	}

	public static void main(String[] args) throws Exception {	
		System.exit(ToolRunner.run(new HBaseBulkLoader(), args));
	}

	private static class HBaseBulkLoaderMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
			String[] val =value.toString().split(",");
			final byte[] rowKey = Bytes.toBytes(val[0]);
			ImmutableBytesWritable hKey = new ImmutableBytesWritable(rowKey);
			
			Put put = new Put(rowKey);
			
			put.addColumn(COL_FAM, Bytes.toBytes("cnt"), Bytes.toBytes(val[1]));
			
			context.write(hKey, put);
		}
	}
}
