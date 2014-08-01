package com.xingcloud.nba.mr.job;

import com.xingcloud.nba.mr.inputformat.MyCombineFileInputFormat;
import com.xingcloud.nba.mr.mapper.ActiveMapper;
import com.xingcloud.nba.mr.model.JoinData;
import com.xingcloud.nba.mr.reducer.ActiveReducer;
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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
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

    private String date1;       //ex:2014-07-29
    private String date2;       //ex:20140729
    private String specialTask;
    private String streamLogPath;
    private String mysqlIdMapPath;
    private String outputPath;


    public ActiveJob(String specialTask, List<String> projects) {
        this.specialTask = specialTask;
        this.projects = projects;
        this.date1 = DateManager.getDaysBefore(1, 0);
        this.date2 = DateManager.getDaysBefore(1, 1);
        this.streamLogPath = fixPath + "/stream_log/pid/" + date1 + "/";
        this.mysqlIdMapPath = fixPath + "/mysqlidmap/";
        this.outputPath = fixPath + "offline/uid/" + specialTask + "/" + date2 + "/all";
    }

    @Override
    public void run() {
        try {
            Configuration conf = new Configuration();
            conf.set("mapred.max.split.size", "524288000");
            Job job = new Job(conf, "Active_" + specialTask);
            conf.setBoolean("mapred.compress.map.output", true);
            conf.setClass("mapred.map.output.compression.codec",Lz4Codec.class, CompressionCodec.class);

            String slPath = "";
            String mimPath = "";

            for(String project : projects) {
                slPath = streamLogPath + project + "/";
                mimPath = mysqlIdMapPath + "vf_" + project + "/id_map.txt";
                FileInputFormat.addInputPaths(job, slPath);
                FileInputFormat.addInputPaths(job, mimPath);

                slPath = "";
                mimPath = "";
            }

            final FileSystem fileSystem = FileSystem.get(new URI(streamLogPath), conf);
            if(fileSystem.exists(new Path(outputPath))) {
                fileSystem.delete(new Path(outputPath), true);
            }

            job.setInputFormatClass(MyCombineFileInputFormat.class);
            job.setMapperClass(ActiveMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(JoinData.class);

            job.setReducerClass(ActiveReducer.class);
            job.setNumReduceTasks(6);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            job.setOutputFormatClass(TextOutputFormat.class);

            job.setJarByClass(ActiveJob.class);
            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
