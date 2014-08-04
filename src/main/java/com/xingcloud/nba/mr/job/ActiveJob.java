package com.xingcloud.nba.mr.job;

import com.xingcloud.nba.mr.mapper.ActiveMapper;
import com.xingcloud.nba.mr.mapper.AnalyzeMapper;
import com.xingcloud.nba.mr.model.JoinData;
import com.xingcloud.nba.mr.reducer.ActiveReducer;
import com.xingcloud.nba.mr.reducer.AnalyzeReducer;
import com.xingcloud.nba.utils.Constant;
import com.xingcloud.nba.utils.DateManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.net.URI;
import java.util.*;

/**
 * Created by wanghaixing on 14-7-29.
 */
public class ActiveJob implements Runnable {
    private static Log LOG = LogFactory.getLog(ActiveJob.class);
    private static String fixPath = "hdfs://ELEX-LA-WEB1:19000/user/hadoop/";

    private List<String> projects;

    private String date;       //ex:2014-07-29
    private String specialTask;
    private String inputPath;
    private String outputPath;
    private int activeType;

    private long count;

    public ActiveJob(String specialTask, int activeType) {
        this.specialTask = specialTask;
        this.activeType = activeType;
        this.inputPath = fixPath + "offline/uid/" + specialTask + "/";
        this.outputPath = fixPath + "offline/uid/" + specialTask + "/all/";
    }

    @Override
    public void run() {
        try {
            Configuration conf = new Configuration();
            conf.set("mapred.max.split.size", "157286400");
            Job job = new Job(conf, "Active_" + specialTask);
            conf.set("mapred.map.child.java.opts", "-Xmx1024m");
            conf.set("mapred.reduce.child.java.opts", "-Xmx1024m");
            conf.set("io.sort.mb", "64");
            conf.setBoolean("mapred.compress.map.output", true);
            conf.setClass("mapred.map.output.compression.codec",Lz4Codec.class, CompressionCodec.class);

            String inPath = "";
            if(activeType == Constant.DAY_ACTIVE_COUNT) {
                date = DateManager.getDaysBefore(1, 1);
                inPath = inputPath + date + "/";
                FileInputFormat.addInputPaths(job, inPath);

            } else if (activeType == Constant.WEEK_ACTIVE_COUNT) {
                for(int i = 1; i <= 7; i++) {
                    date = DateManager.getDaysBefore(i, 1);
                    inPath = inputPath + date + "/";
                    FileInputFormat.addInputPaths(job, inPath);
                    inPath = "";
                }

            } else if (activeType == Constant.MONTH_ACTIVE_COUNT) {
                for(int i = 1; i <= 30; i++) {
                    date = DateManager.getDaysBefore(i, 1);
                    inPath = inputPath + date + "/";
                    FileInputFormat.addInputPaths(job, inPath);
                    inPath = "";
                }
            }

            final FileSystem fileSystem = FileSystem.get(new URI(outputPath), conf);
            if(fileSystem.exists(new Path(outputPath))) {
                fileSystem.delete(new Path(outputPath), true);
            }

            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(ActiveMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setReducerClass(ActiveReducer.class);
            job.setNumReduceTasks(6);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            job.setOutputFormatClass(TextOutputFormat.class);

            job.setJarByClass(ActiveJob.class);
            job.waitForCompletion(true);


            Counters counters = job.getCounters();
            count = counters.findCounter("ActiveCounter", "uidCounts").getValue();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

}
