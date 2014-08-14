package com.xingcloud.nba.mr.job;

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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;

/**
 * Created by wanghaixing on 14-8-13.
 */
public class CalcNewUserJob implements Runnable {
    private static Log LOG = LogFactory.getLog(CalcNewUserJob.class);
    private static String fixPath = "hdfs://ELEX-LA-WEB1:19000/user/hadoop/";

    private String date;
    private String specialTask;
    private String inputPath;
    private String outputPath;
    private long count;

    public CalcNewUserJob(String specialTask) {
        date = DateManager.getDaysBefore(4, 0);
        this.specialTask = specialTask;
        inputPath = fixPath + "whx/reguid/" + specialTask + "/";
        outputPath = fixPath + "offline/retuid/day/" + specialTask + "/" + date + "/";
    }

    public void run() {
        try {
            Configuration conf = new Configuration();
            Job job = new Job(conf, "CalcNewUserJob_" + specialTask);
            conf.set("mapred.map.child.java.opts", "-Xmx1024m");
            conf.set("mapred.reduce.child.java.opts", "-Xmx1024m");
            conf.set("io.sort.mb", "64");

            FileInputFormat.addInputPaths(job, inputPath);
            final FileSystem fileSystem = FileSystem.get(new URI(outputPath), conf);
            if(fileSystem.exists(new Path(outputPath))) {
                fileSystem.delete(new Path(outputPath), true);
            }

            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(CalcNewUserMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setReducerClass(CalcNewUserReducer.class);
            job.setNumReduceTasks(5);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            job.setOutputFormatClass(TextOutputFormat.class);

            job.setJarByClass(CalcNewUserJob.class);
            job.waitForCompletion(true);

            Counters counters = job.getCounters();
            count = counters.findCounter("reg", "num").getValue();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static class CalcNewUserMapper extends Mapper<LongWritable, Text, Text, Text> {
        protected void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {
            String date2 = DateManager.getDaysBefore(4, 1);
            String[] items = value.toString().split("\t");
            if(items[1].trim().startsWith(date2)) {
                context.write(new Text(items[0]), new Text(items[1]));
            }
            /*if(items[1].trim().startsWith("201408")){
                context.getCounter("regist",items[1].substring(0,8)).increment(1);
            }*/
        }
    }

    static class CalcNewUserReducer extends Reducer<Text, Text, Text, NullWritable> {
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
            context.getCounter("reg", "num").increment(1L);
        }
    }

    public long getCount() { return count; }

    public void setCount(long count) {
        this.count = count;
    }

}
