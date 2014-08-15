package com.xingcloud.nba.mr.job;

import com.xingcloud.nba.mr.mapper.AnalyzeMapper;
import com.xingcloud.nba.mr.reducer.AnalyzeReducer;
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
import org.apache.hadoop.mapreduce.*;
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
 * Created by Administrator on 14-8-7.
 */
public class BeUniqJob implements Runnable {
    private static Log LOG = LogFactory.getLog(AnalyzeJob.class);
    private static String fixPath = "hdfs://ELEX-LA-WEB1:19000/user/hadoop/";

    private String date1;
    private String specialTask;
    private String inputPath;
    private String outputPath;

    public BeUniqJob(String specialTask) {
        this.specialTask = specialTask;
        this.date1 = DateManager.getDaysBefore(8, 0);
        this.inputPath = fixPath + "offline/uid/" + specialTask + "/";
        this.outputPath = fixPath + "offline/retuid/week/" + specialTask + "/" + date1 + "/";
    }

    public void run() {
        try {
            Configuration conf = new Configuration();
            Job job = new Job(conf, "BeUniq_" + specialTask);
            conf.set("mapred.map.child.java.opts", "-Xmx1024m");
            conf.set("mapred.reduce.child.java.opts", "-Xmx1024m");
            conf.set("io.sort.mb", "64");
            conf.setBoolean("mapred.compress.map.output", true);
            conf.setClass("mapred.map.output.compression.codec",Lz4Codec.class, CompressionCodec.class);

            String inPath = "";
            String date2 = "";
            for(int i = 1; i <= 7; i++) {
                date2 = DateManager.getDaysBefore(i, 1);
                inPath = inputPath + date2 + "/";
                FileInputFormat.addInputPaths(job, inPath);
            }

            final FileSystem fileSystem = FileSystem.get(new URI(outputPath), conf);
            if(fileSystem.exists(new Path(outputPath))) {
                fileSystem.delete(new Path(outputPath), true);
            }

            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(AnalyzeMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setReducerClass(AnalyzeReducer.class);
            job.setNumReduceTasks(5);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            job.setOutputFormatClass(TextOutputFormat.class);

            job.setJarByClass(BeUniqJob.class);
            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
