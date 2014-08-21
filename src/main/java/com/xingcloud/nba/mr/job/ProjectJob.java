package com.xingcloud.nba.mr.job;

import com.xingcloud.nba.mr.inputformat.MyCombineFileInputFormat;
import com.xingcloud.nba.mr.mapper.ActiveMapper;
import com.xingcloud.nba.mr.mapper.ProjectMapper;
import com.xingcloud.nba.mr.model.JoinData;
import com.xingcloud.nba.mr.reducer.ActiveReducer;
import com.xingcloud.nba.mr.reducer.ProjectReducer;
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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.util.List;

/**
 * Created by Administrator on 14-8-2.
 */
public class ProjectJob implements Runnable {
    private static Log LOG = LogFactory.getLog(ActiveJob.class);
    private static String fixPath = "hdfs://ELEX-LA-WEB1:19000/user/hadoop/";

    private String date;       //ex:2014-07-29
    private String specialTask;
    private String project;
    private String streamLogPath;
    private String mysqlIdMapPath;
    private String outputPath;


    public ProjectJob(String specialTask, String project) {
        this.specialTask = specialTask;
        this.project = project;
        this.date = DateManager.getDaysBefore(1, 0);       //ex:2014-07-29
        this.streamLogPath = fixPath + "stream_log/pid/" + date + "/" + project + "/";
        this.mysqlIdMapPath = fixPath + "mysqlidmap/" + "vf_" + project + "/id_map.txt";
        this.outputPath = fixPath + "whx/uid/" + date + "/" + specialTask + "/" + project + "/";
    }

    @Override
    public void run() {
        try {
            Configuration conf = new Configuration();
            conf.set("mapred.max.split.size", "157286400");
            Job job = new Job(conf, specialTask + "_" + project);
            conf.set("mapred.map.child.java.opts", "-Xmx1024m");
            conf.set("mapred.reduce.child.java.opts", "-Xmx1024m");
            conf.set("io.sort.mb", "64");
            conf.set("projectName", project);
            conf.setBoolean("mapred.compress.map.output", true);
            conf.setClass("mapred.map.output.compression.codec",Lz4Codec.class, CompressionCodec.class);


            FileInputFormat.addInputPaths(job, streamLogPath);
            FileInputFormat.addInputPaths(job, mysqlIdMapPath);

            final FileSystem fileSystem = FileSystem.get(new URI(streamLogPath), conf);
            if(fileSystem.exists(new Path(outputPath))) {
                fileSystem.delete(new Path(outputPath), true);
            }

            job.setInputFormatClass(MyCombineFileInputFormat.class);
            job.setMapperClass(ProjectMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(JoinData.class);

            job.setReducerClass(ProjectReducer.class);
            job.setNumReduceTasks(6);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            job.setOutputFormatClass(TextOutputFormat.class);

            job.setJarByClass(ProjectJob.class);
            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
