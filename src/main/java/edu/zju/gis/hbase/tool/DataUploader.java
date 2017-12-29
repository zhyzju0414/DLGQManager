
package edu.zju.gis.hbase.tool;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.Geometry.Type;

import scala.reflect.io.Directory;

/*
 * hbase表数据上传工具  zxw
 */
public class DataUploader {

    private static final String NAME = "DataUploader";

    static class Uploader
            extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

        private long checkpoint = 100;
        private long count = 0;

        @Override
        public void map(LongWritable key, Text line, Context context)
                throws IOException {

            String [] values = line.toString().split(Utils.DEFAULT_INPUT_DELIMITER);


            // Create Put
            Put put = Utils.getDataRecordPut(values, 20151115);


            try {
                context.write(new ImmutableBytesWritable(put.getRow()), put);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // Set status every checkpoint lines
            if(++count % checkpoint == 0) {
                context.setStatus("Emitting Put " + count);
            }
        }
    }

    /**
     * Job configuration.
     */
    public static Job configureJob(Configuration conf, String [] args)
            throws IOException {
        Path inputPath = new Path(args[0]);
        String tableName = args[1];
        Job job = new Job(conf, NAME + "_" + tableName);
        job.setJarByClass(Uploader.class);
        FileInputFormat.addInputPath(job, inputPath);

        job.setMapperClass(Uploader.class);
        // No reducers.  Just write straight to table.  Call initTableReducerJob
        // because it sets up the TableOutputFormat.
        TableMapReduceUtil.initTableReducerJob(tableName, null, job);
        job.setNumReduceTasks(0);
        return job;
    }

    /**
     * Main entry point.
     *
     * @param args  The command line parameters.
     * @throws Exception When running the job fails.
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 2) {
            System.err.println("Wrong number of arguments: " + otherArgs.length);
            System.err.println("Usage: " + NAME + " <input> <tablename>");
            System.exit(-1);
        }
        Job job = configureJob(conf, otherArgs);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }





}
