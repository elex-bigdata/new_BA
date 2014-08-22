package com.xingcloud.nba.mr.job;

import com.xingcloud.nba.mr.model.JoinData;
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
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by wanghaixing on 14-8-22.
 */
public class OneDayRetJob implements Runnable {
    private static Log LOG = LogFactory.getLog(OneDayRetJob.class);
    private static String fixPath = "hdfs://ELEX-LA-WEB1:19000/user/hadoop/";

    private String newUserPath;
    private String activePath;
    private String outputPath;
    private String regDate;
    private String date;
    private int type;   //留存类型：二日或七日
    private String specialTask;
    private long count;

    public OneDayRetJob(String specialTask, int type) {
        this.specialTask = specialTask;
        this.type = type;
        date = DateManager.getDaysBefore(1, 0);
        if(type == Constant.TWO_RET) {
            regDate = DateManager.getDaysBefore(2, 0);
            this.outputPath = fixPath + "whx/retuid2/" + specialTask + "/" + date + "/";
        } else if(type == Constant.SEVEN_RET) {
            regDate = DateManager.getDaysBefore(7, 0);
            this.outputPath = fixPath + "whx/retuid7/" + specialTask + "/" + date + "/";
        }
        this.newUserPath = fixPath + "offline/retuid/day/" + specialTask + "/" + regDate + "/";
        this.activePath = fixPath + "offline/uid/" + specialTask + "/" + date + "/";
    }

    public void run() {
        try {
            Configuration conf = new Configuration();
            conf.set("mapred.max.split.size", "157286400");
            conf.set("mapred.map.child.java.opts", "-Xmx1024m");
            conf.set("mapred.reduce.child.java.opts", "-Xmx1024m");
            conf.set("io.sort.mb", "64");
            Job job = new Job(conf, "Retention_" + specialTask);

            FileInputFormat.addInputPaths(job, activePath);
            FileInputFormat.addInputPaths(job, newUserPath);

            final FileSystem fileSystem = FileSystem.get(new URI(outputPath), conf);
            if(fileSystem.exists(new Path(outputPath))) {
                fileSystem.delete(new Path(outputPath), true);
            }

            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(OneDayRetMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(JoinData.class);

            job.setReducerClass(OneDayRetReducer.class);
            job.setNumReduceTasks(5);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            job.setOutputFormatClass(TextOutputFormat.class);

            job.setJarByClass(OneDayRetJob.class);
            job.waitForCompletion(true);

            Counters counters = job.getCounters();
            count = counters.findCounter("Retention", "Retnum").getValue();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static class OneDayRetMapper extends Mapper<LongWritable, Text, Text, JoinData> {
        private JoinData joinData = new JoinData();

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String pathName = ((FileSplit)context.getInputSplit()).getPath().toString();
            Text joinKey = new Text();
            Text flag = new Text();
            Text secondPart = new Text();

            if (pathName.contains("offline/retuid/day/")) {
                String items = value.toString();
                if(items != null && !items.trim().equals("")){
                    flag.set("0");
                    joinKey.set(items);
                    secondPart.set(items);
                    joinData = new JoinData(joinKey, flag, secondPart);
                    context.getCounter("day","log").increment(1);
                    context.write(joinKey, joinData);
                } else {
                    context.getCounter("day","nulllog").increment(1);
                }
            } else if (pathName.contains("offline/uid/")) {
                String items = value.toString();
                if(items != null && !items.trim().equals("")){
                    flag.set("1");
                    joinKey.set(items);
                    secondPart.set(items);
                    joinData = new JoinData(joinKey, flag, secondPart);
                    context.getCounter("act","log").increment(1);
                    context.write(joinKey, joinData);
                } else {
                    context.getCounter("act","nulllog").increment(1);
                }
            }

        }
    }

    static class OneDayRetReducer extends Reducer<Text, JoinData, Text, NullWritable> {

        protected void reduce(Text key, Iterable<JoinData> values, Context context) throws IOException, InterruptedException {
            Set<String> firstTable = new HashSet<String>();
            Set<String> secondTable = new HashSet<String>();
            Text secondPart = null;
            Text output = new Text();
            String flag;

            for(JoinData jd : values) {
                flag = jd.getFlag().toString().trim();
                secondPart = jd.getSecondPart();
                if("0".equals(flag)) {
                    firstTable.add(secondPart.toString());
                } else if("1".equals(flag)) {
                    secondTable.add(secondPart.toString());
                }
            }

            if(firstTable.size() > 0) {
                for(String uid : firstTable) {
                    for(String orgid : secondTable) {
                        context.getCounter("Retention", "Retnum").increment(1L);
                        output.set(orgid);
                        context.write(output, NullWritable.get());
                    }
                }
            }

        }

    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
}
