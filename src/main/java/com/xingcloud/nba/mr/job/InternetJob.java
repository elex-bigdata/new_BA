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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.net.URI;

/**
 * 对internet-1和internet-2中的数据去重，生成internet中的数据
 * 这里暂时有两个方面数据的去重：日活跃uid、每日新增用户uid，根据type来定
 * Created by wanghaixing on 14-8-4.
 */
public class InternetJob implements Runnable {
    private static Log LOG = LogFactory.getLog(InternetJob.class);
    private static String fixPath = "hdfs://ELEX-LA-WEB1:19000/user/hadoop/";

    private String date = DateManager.getDaysBefore(1, 1);
    private String inputPath1;  //internet-1
    private String inputPath2;  //internet-2
    private String outputPath;

    public InternetJob() {}

    public InternetJob(int type) {
        if(Constant.ACT_UNIQ == type) {
            date = DateManager.getDaysBefore(1, 1);
            inputPath1 = fixPath + "offline/uid/internet-1/" + date + "/";
            inputPath2 = fixPath + "offline/uid/internet-2/" + date + "/";
            outputPath = fixPath + "offline/uid/internet/" + date + "/";
        } else if(Constant.NEW_UNIQ == type) {
            date = DateManager.getDaysBefore(1, 0);
            inputPath1 = fixPath + "offline/retuid/day/internet-1/" + date + "/";
            inputPath2 = fixPath + "offline/retuid/day/internet-2/" + date + "/";
            outputPath = fixPath + "offline/retuid/day/internet/" + date + "/";
        }
    }

    public void run() {
        try {
            Configuration conf = new Configuration();
            Job job = new Job(conf, "InternetJob");

            final FileSystem fileSystem = FileSystem.get(new URI(inputPath1), conf);
            if(fileSystem.exists(new Path(outputPath))) {
                fileSystem.delete(new Path(outputPath), true);
            }

            FileInputFormat.addInputPaths(job, inputPath1);
            FileInputFormat.addInputPaths(job, inputPath2);

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

            job.setJarByClass(InternetJob.class);
            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
