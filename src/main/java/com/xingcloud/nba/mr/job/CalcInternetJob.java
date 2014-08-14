package com.xingcloud.nba.mr.job;

import com.xingcloud.nba.utils.Constant;
import com.xingcloud.nba.utils.DateManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Created by Administrator on 14-8-14.
 */
public class CalcInternetJob implements Runnable {
    private static Log LOG = LogFactory.getLog(CalcNewUserJob.class);
    private static String fixPath = "hdfs://ELEX-LA-WEB1:19000/user/hadoop/";

    private String date;
    private String inputPath1;
    private String inputPath2;
    private String outputPath;
    private long count;

    public CalcInternetJob() {
        date = DateManager.getDaysBefore(9, 0);
        inputPath1 = fixPath + "whx/reguid/internet-1/";
        inputPath2 = fixPath + "whx/reguid/internet-2/";
        outputPath = fixPath + "offline/retuid/day/internet/" + date + "/";
    }

    public void run() {
        try {
            Configuration conf = new Configuration();
            Job job = new Job(conf, "CalcInternetJob");
            conf.set("mapred.map.child.java.opts", "-Xmx1024m");
            conf.set("mapred.reduce.child.java.opts", "-Xmx1024m");
            conf.set("io.sort.mb", "64");
            conf.setBoolean("mapred.compress.map.output", true);
            conf.setClass("mapred.map.output.compression.codec",Lz4Codec.class, CompressionCodec.class);

            FileInputFormat.addInputPaths(job, inputPath1);
            FileInputFormat.addInputPaths(job, inputPath2);

            final FileSystem fileSystem = FileSystem.get(new URI(outputPath), conf);
            if(fileSystem.exists(new Path(outputPath))) {
                fileSystem.delete(new Path(outputPath), true);
            }

            Configuration mapconf1 = new Configuration(false);
            ChainMapper.addMapper(job, CalcInternetMapper1.class, LongWritable.class, Text.class, Text.class, Text.class, mapconf1);
            Configuration reduceconf = new Configuration(false);
            ChainReducer.setReducer(job, CalcInternetReducer.class, Text.class, Text.class, Text.class, Text.class, reduceconf);
            Configuration mapconf2 = new Configuration(false);
            ChainReducer.addMapper(job, CalcInternetMapper2.class, Text.class, Text.class, Text.class, NullWritable.class, mapconf2);

            job.setInputFormatClass(TextInputFormat.class);
            job.setNumReduceTasks(5);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            job.setOutputFormatClass(TextOutputFormat.class);

            job.setJarByClass(CalcInternetJob.class);
            job.waitForCompletion(true);

            Counters counters = job.getCounters();
            count = counters.findCounter("interReg", "num").getValue();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static class CalcInternetMapper1 extends Mapper<LongWritable, Text, Text, Text> {
        protected void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {
            String[] items = value.toString().split("\t");
            context.write(new Text(items[0]), new Text(items[1]));
        }
    }

    static class CalcInternetReducer extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String min = "99999999999999";
            String time = "";
            for(Text value : values) {
                time = value.toString().trim();
                if(min.compareTo(time) > 0) {
                    min = time;
                }
            }
            context.write(key, new Text(min));
        }

    }

    static class CalcInternetMapper2 extends Mapper<Text, Text, Text, NullWritable> {
        protected void map(Text key, Text value, Context context) throws IOException,InterruptedException {
            String date2 = DateManager.getDaysBefore(9, 1);
            String items = value.toString();
            if(items.trim().startsWith(date2)) {
                context.write(key, NullWritable.get());
                context.getCounter("interReg", "num").increment(1L);
            }
            if(items.startsWith("201408")){
                context.getCounter("regist",items.substring(0,8)).increment(1);
            }
        }
    }

    public long getCount() { return count; }

    public void setCount(long count) {
        this.count = count;
    }
}
