package edu.zju.gis.hbase.tool;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.GenericOptionsParser;

import com.esri.core.geometry.Envelope;

import edu.zju.gis.hadoop.tool.CombineSmallfileInputFormat;
import edu.zju.gis.hbase.tool.DataUploader.Uploader;

/*
 * 基于MapReduce并行构建多级四叉树空间索引 zxw
 */
public class SpatialIndexBuilder {

    public static class Map extends
            Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put> {

        public ImmutableBytesWritable tableName;


        @Override
        protected void map(ImmutableBytesWritable rowKey, Result result, Context context)
                throws IOException, InterruptedException {
            byte[] value = result.getValue(Utils.COLUMNFAMILY_PROPERTY, Utils.MBR_QUALIFIER);
            String mbrStr = Bytes.toString(value);
            Envelope mbr = Utils.GetEnvelope(mbrStr);

            byte[] indexrowkey =Utils.GetIndexRow(mbr, Utils.UNIVERSEGRID._1(), Utils.ENDLEVEL);
            String category = Utils.GetCategoryCode(Bytes.toString( rowKey.get()));
            if(indexrowkey!=null){

                Put indexPut = new Put(indexrowkey);
                indexPut.add(Bytes.toBytes(category.substring(0, 2)),  rowKey.get(),20151115,  rowKey.get());
                context.write(tableName, indexPut);
            }
        }

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            Configuration configuration = context.getConfiguration();
            tableName= new ImmutableBytesWritable(Bytes.toBytes( configuration.get("index.tablename")));
        }
    }

    /**
     * Job configuration.
     */
    public static Job configureJob(Configuration conf, String [] args)
            throws IOException {
        String tableName = args[0];        //表名称
        System.out.println("****" + tableName);
        // conf.set(TableInputFormat.SCAN, TableMapReduceUtil.convertScanToString(new Scan()));
        conf.set(TableInputFormat.INPUT_TABLE, tableName);
        conf.set("index.tablename",  Utils.GetSpatialIndexTableNameByDataTableName(tableName));
        Job job = new Job(conf,  tableName);
        job.setJarByClass(SpatialIndexBuilder.class);
        job.setMapperClass(Map.class);
        job.setNumReduceTasks(0);
        job.setInputFormatClass(TableInputFormat.class);
        job.setOutputFormatClass(MultiTableOutputFormat.class);
        return job;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length < 1) {
            System.err.println("Only " + otherArgs.length + " arguments supplied, required: 1");
            System.err.println("Usage: IndexBuilder <TABLE_NAME> ");
            System.exit(-1);
        }
        Job job = configureJob(conf, otherArgs);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
