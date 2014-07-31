package com.xingcloud.nba.mr.job;

import com.xingcloud.nba.mr.inputformat.MyCombineFileInputFormat;
import com.xingcloud.nba.mr.mapper.ActiveMapper;
import com.xingcloud.nba.mr.model.JoinData;
import com.xingcloud.nba.mr.reducer.ActiveReducer;
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
import java.text.SimpleDateFormat;
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
        this.date1 = getYesterday(0);
        this.date2 = getYesterday(1);
        this.streamLogPath = fixPath + "/stream_log/pid/" + date1 + "/";
        this.mysqlIdMapPath = fixPath + "/mysqlidmap/";
        this.outputPath = fixPath + "offline/uid/" + specialTask + "/" + date2 + "/";
    }

    @Override
    public void run() {
        try {
            Configuration conf = new Configuration();
            conf.set("mapred.max.split.size", "524288000");
            Job activeJob = new Job(conf, specialTask);
            conf.setBoolean("mapred.compress.map.output", true);
            conf.setClass("mapred.map.output.compression.codec",Lz4Codec.class, CompressionCodec.class);

            String slPath = "";
            String mimPath = "";

            for(String project : projects) {
                slPath = streamLogPath + project + "/";
                mimPath = mysqlIdMapPath + "vf_" + project + "/id_map.txt";
                FileInputFormat.addInputPaths(activeJob, slPath);
                FileInputFormat.addInputPaths(activeJob, mimPath);

                slPath = "";
                mimPath = "";
            }

            final FileSystem fileSystem = FileSystem.get(new URI(streamLogPath), conf);
            if(fileSystem.exists(new Path(outputPath))) {
                fileSystem.delete(new Path(outputPath), true);
            }

            activeJob.setInputFormatClass(MyCombineFileInputFormat.class);
            activeJob.setMapperClass(ActiveMapper.class);
            activeJob.setMapOutputKeyClass(Text.class);
            activeJob.setMapOutputValueClass(JoinData.class);

            activeJob.setReducerClass(ActiveReducer.class);
            activeJob.setNumReduceTasks(6);
            activeJob.setOutputKeyClass(Text.class);
            activeJob.setOutputValueClass(NullWritable.class);
            FileOutputFormat.setOutputPath(activeJob, new Path(outputPath));
            activeJob.setOutputFormatClass(TextOutputFormat.class);

            activeJob.setJarByClass(ActiveJob.class);
            activeJob.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String getYesterday(int type) {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, -1);
        SimpleDateFormat sdf = null;
        if(type == 0) {
            sdf = new SimpleDateFormat("yyyy-MM-dd");
        } else {
            sdf = new SimpleDateFormat("yyyyMMdd");
        }

        return sdf.format(cal.getTime());
    }
}
