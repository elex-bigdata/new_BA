package com.xingcloud.nba.mr.job;

import com.xingcloud.nba.mr.inputformat.MyCombineFileInputFormat;
import com.xingcloud.nba.mr.model.JoinData;
import com.xingcloud.nba.utils.Constant;
import com.xingcloud.nba.utils.DateManager;
import com.xingcloud.xa.uidmapping.UidMappingUtil;
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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

/**
 * 将internet-1和internet-2中每个项目在mysql中的加密uid转换成项目对应的uid，
 * 生成的文件在取得当天注册的原始uid后会删掉
 * Created by wanghaixing on 14-8-13.
 */
public class TransUidJob implements Runnable {
    private static Log LOG = LogFactory.getLog(RegUidJob.class);
    private static String fixPath = "hdfs://ELEX-LA-WEB1:19000/user/hadoop/";

    private String mysqlPath;
    private String mysqlIdMapPath;
    private String outputPath;

    private String specialTask;
    private String project;

    public TransUidJob(String specialTask, String project) {
        this.specialTask = specialTask;
        this.project = project;
        this.mysqlPath = fixPath + "mysql/" + project;
        this.mysqlIdMapPath = fixPath + "mysqlidmap/" + "vf_" + project + "/id_map.txt";
        this.outputPath = fixPath + "whx/transuid/" + specialTask + "/" + project + "/";
    }

    public void run() {
        try {
            Configuration conf = new Configuration();
            conf.set("mapred.max.split.size", "157286400");
            conf.set("mapred.map.child.java.opts", "-Xmx1024m");
            conf.set("mapred.reduce.child.java.opts", "-Xmx1024m");
            conf.set("io.sort.mb", "64");
            conf.setBoolean("mapred.compress.map.output", true);
            conf.setClass("mapred.map.output.compression.codec",Lz4Codec.class, CompressionCodec.class);
            Job job = new Job(conf, "TransUidJob_" + project);

            FileSystem fileSystem = FileSystem.get(new URI(mysqlPath), conf);
            String inPath = "";
            for(int i = 0; i < 16; i++) {
                inPath = mysqlPath + "/node" + i + "_register_time.log";
                if(fileSystem.exists(new Path(inPath))) {
                    FileInputFormat.addInputPaths(job, inPath);
                }
            }
            FileInputFormat.addInputPaths(job, mysqlIdMapPath);

            if(fileSystem.exists(new Path(outputPath))) {
                fileSystem.delete(new Path(outputPath), true);
            }

            job.setInputFormatClass(MyCombineFileInputFormat.class);
            job.setMapperClass(TransUidMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(JoinData.class);

            job.setReducerClass(TransUidReducer.class);
            job.setNumReduceTasks(3);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            job.setOutputFormatClass(TextOutputFormat.class);

            job.setJarByClass(TransUidJob.class);
            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    static class TransUidMapper extends Mapper<LongWritable, Text, Text, JoinData> {
        private JoinData joinData = new JoinData();

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Text joinKey = new Text();
            Text flag = new Text();
            Text secondPart = new Text();

            if (key.get() == Constant.KEY_FOR_MYSQL) {
                String[] items = value.toString().split("\t");
                if(items != null && !items[1].trim().equals("")){
                    /*long uid = Long.parseLong(items[0].toString());
                    Long[] transuids = new Long[1];
                    try {
                        transuids = HbaseMysqlUIDTruncator.truncate(uid);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }*/
                    flag.set("0");
                    joinKey.set(String.valueOf(items[0]));
                    secondPart.set(items[1]);
                    joinData = new JoinData(joinKey, flag, secondPart);
                    context.getCounter("mysql","log").increment(1);
                    context.write(joinKey, joinData);
                } else {
                    context.getCounter("mysql","miss").increment(1);
                    System.out.println(value);
                }


            } else if (key.get() == Constant.KEY_FOR_IDMAP) {
                context.getCounter("idmapyyy","logyyy").increment(1);
                String[] items = value.toString().split("\t");
                if(items.length == 2) {
                    long uid = 0;
                    try {
                        uid = Long.parseLong(items[0].toString());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    long lo = UidMappingUtil.getInstance().decorateWithMD5(uid);
                    flag.set("1");
                    joinKey.set(String.valueOf(lo));
                    secondPart.set(items[1]);
                } else {
                    /*flag.set("1");
                    joinKey.set(items[0]);
                    secondPart.set(new Text("***"));*/
                }
                joinData = new JoinData(joinKey, flag, secondPart);
                context.write(joinKey, joinData);
            }

        }
    }

    static class TransUidReducer extends Reducer<Text, JoinData, Text, Text> {

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
                for(String time : firstTable) {
                    for(String orgid : secondTable) {
                        output.set(orgid);
                        context.write(output, new Text(time));
                    }
                }
            }

        }

    }
}
