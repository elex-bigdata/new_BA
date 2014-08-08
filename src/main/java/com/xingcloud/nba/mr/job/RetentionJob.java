package com.xingcloud.nba.mr.job;

import com.xingcloud.nba.mr.inputformat.MyCombineFileInputFormat;
import com.xingcloud.nba.mr.model.JoinData;
import com.xingcloud.nba.utils.Constant;
import com.xingcloud.nba.utils.DateManager;
import com.xingcloud.uidtransform.HbaseMysqlUIDTruncator;
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by Administrator on 14-8-8.
 */
public class RetentionJob {
    private static Log LOG = LogFactory.getLog(RegUidJob.class);
    private static String fixPath = "hdfs://ELEX-LA-WEB1:19000/user/hadoop/";

    private String dayPath;
    private String weekPath;
    private String outputPath;
    private String date;
    private String specialTask;
    private long count;

    public RetentionJob(String specialTask) {
        date = DateManager.getDaysBefore(7, 0);
        this.specialTask = specialTask;
        this.dayPath = fixPath + "offline/retuid/day/" + specialTask + "/" + date;
        this.weekPath = fixPath + "offline/retuid/week/" + specialTask + "/" + date;
        this.outputPath = fixPath + "whx/transuid/" + date + "/temp/";
    }

    public void run() {
        try {
            Configuration conf = new Configuration();
            conf.set("mapred.max.split.size", "157286400");
            Job job = new Job(conf, "Retention_" + specialTask);
            conf.set("mapred.map.child.java.opts", "-Xmx1024m");
            conf.set("mapred.reduce.child.java.opts", "-Xmx1024m");
            conf.set("io.sort.mb", "64");
//            conf.setBoolean("mapred.compress.map.output", true);
//            conf.setClass("mapred.map.output.compression.codec",Lz4Codec.class, CompressionCodec.class);


            FileInputFormat.addInputPaths(job, dayPath);
            FileInputFormat.addInputPaths(job, weekPath);

            final FileSystem fileSystem = FileSystem.get(new URI(outputPath), conf);
            if(fileSystem.exists(new Path(outputPath))) {
                fileSystem.delete(new Path(outputPath), true);
            }

            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(RetentionMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(JoinData.class);

            job.setReducerClass(RetentionReducer.class);
            job.setNumReduceTasks(5);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            job.setOutputFormatClass(TextOutputFormat.class);

            job.setJarByClass(RetentionJob.class);
            job.waitForCompletion(true);

            Counters counters = job.getCounters();
            count = counters.findCounter("Retention", "Retnum").getValue();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    static class RetentionMapper extends Mapper<LongWritable, Text, Text, JoinData> {
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
                    context.getCounter("day","log").increment(1);
                }
            } else if (pathName.contains("offline/retuid/week/")) {
                String items = value.toString();
                if(items != null && !items.trim().equals("")){
                    flag.set("1");
                    joinKey.set(items);
                    secondPart.set(items);
                    joinData = new JoinData(joinKey, flag, secondPart);
                    context.getCounter("week","log").increment(1);
                    context.write(joinKey, joinData);
                } else {
                    context.getCounter("week","log").increment(1);
                }
            }

        }
    }

    static class RetentionReducer extends Reducer<Text, JoinData, Text, NullWritable> {

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
