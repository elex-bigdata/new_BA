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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by Administrator on 14-8-6.
 */
public class RegUidJob implements Runnable {
    private static Log LOG = LogFactory.getLog(RegUidJob.class);
    private static String fixPath = "hdfs://ELEX-LA-WEB1:19000/user/hadoop/";

    private String mysqlPath;
    private String mysqlIdMapPath;
    private String outputPath;

    private static String date1;
    private static String date2;
    private String specialTask;
    private String project;

    public RegUidJob(String specialTask, String project) {
        date1 = DateManager.getDaysBefore(7, 0);
        date2 = DateManager.getDaysBefore(7, 1);
        this.specialTask = specialTask;
        this.project = project;
        this.mysqlPath = fixPath + "mysql/" + project;
        this.mysqlIdMapPath = fixPath + "mysqlidmap/" + "vf_" + project + "/id_map.txt";
        this.outputPath = fixPath + "whx/transuid/" + date1 + "/" + specialTask + "/" + project + "/";
    }

    public void run() {
        try {
            Configuration conf = new Configuration();
            conf.set("mapred.max.split.size", "157286400");
            Job job = new Job(conf, "RegUidJob_" + specialTask);
            conf.set("mapred.map.child.java.opts", "-Xmx1024m");
            conf.set("mapred.reduce.child.java.opts", "-Xmx1024m");
            conf.set("io.sort.mb", "64");

            String inPath = "";
            for(int i = 0; i < 16; i++) {
                inPath = mysqlPath + "/node" + i + "_register_time.log";
                FileInputFormat.addInputPaths(job, inPath);
            }
            FileInputFormat.addInputPaths(job, mysqlIdMapPath);

            final FileSystem fileSystem = FileSystem.get(new URI(mysqlPath), conf);
            if(fileSystem.exists(new Path(outputPath))) {
                fileSystem.delete(new Path(outputPath), true);
            }

            job.setInputFormatClass(MyCombineFileInputFormat.class);
            job.setMapperClass(RegUidMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(JoinData.class);

            job.setReducerClass(RegUidReducer.class);
            job.setNumReduceTasks(5);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            job.setOutputFormatClass(TextOutputFormat.class);

            job.setJarByClass(AnalyzeJob.class);
            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    static class RegUidMapper extends Mapper<LongWritable, Text, Text, JoinData> {
        private JoinData joinData = new JoinData();

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Text joinKey = new Text();
            Text flag = new Text();
            Text secondPart = new Text();

            if (key.get() == Constant.KEY_FOR_MYSQL) {
                String[] items = value.toString().split("\t");
                if(items[1].startsWith(date2)) {
                    long uid = Long.parseLong(items[0].toString());
                    Long[] transuids = new Long[0];
                    try {
                        transuids = HbaseMysqlUIDTruncator.truncate(uid);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    flag.set("0");
                    joinKey.set(String.valueOf(transuids[0]));
                    secondPart.set(items[0]);
                    joinData = new JoinData(joinKey, flag, secondPart);
                    context.getCounter("mysql","log").increment(1);
                    context.write(joinKey, joinData);
                }

            } else if (key.get() == Constant.KEY_FOR_IDMAP) {
                String[] items = value.toString().split("\t");
                if(items.length == 2) {
                    flag.set("1");
                    joinKey.set(items[0]);
                    secondPart.set(items[1]);
                } else {
                    flag.set("1");
                    joinKey.set(items[0]);
                    secondPart.set(new Text("***"));
                }
                joinData = new JoinData(joinKey, flag, secondPart);
                context.write(joinKey, joinData);
            }

        }
    }

    static class RegUidReducer extends Reducer<Text, JoinData, Text, NullWritable> {
        private Set<String> firstTable = new HashSet<String>();
        private Set<String> secondTable = new HashSet<String>();
        private Text secondPart = null;
        private Text output = new Text();
        private String flag;

        protected void reduce(Text key, Iterable<JoinData> values, Context context) throws IOException, InterruptedException {

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
                        output.set(orgid);
                        context.write(output, NullWritable.get());
                    }
                }
            }

        }

    }
}
