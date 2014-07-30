package com.xingcloud.nba.mr.job;

import com.xingcloud.nba.mr.mapper.ActiveMapper;
import com.xingcloud.nba.mr.model.JoinData;
import com.xingcloud.nba.mr.reducer.ActiveReducer;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by wanghaixing on 14-7-29.
 */
public class ActiveJob {
    private static Log LOG = LogFactory.getLog(ActiveJob.class);
    private static String fixPath = "hdfs://ELEX-LA-WEB1:19000/user/hadoop/";

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job activeJob = new Job(conf, ActiveJob.class.getSimpleName());
            activeJob.setJarByClass(ActiveJob.class);
            conf.setBoolean("mapred.compress.map.output", true);
            conf.setClass("mapred.map.output.compression.codec",GzipCodec.class, CompressionCodec.class);

            ActiveJob jobClass = new ActiveJob();
            String date1 = jobClass.getYesterday(0);
            String date2 = jobClass.getYesterday(1);
            String streamLogPath = fixPath + "/stream_log/pid/" + date1 + "/";
            String mysqlIdMapPath = fixPath + "/mysqlidmap/";
            String outputPath = fixPath + "offline/uid/internet-1/" + date2 + "/";

            String[] specials = {"internet", "internet-1", "internet-2"};
            Map<String, List<String>> specialProjectList = getSpecialProjectList();
            List<String> internet1Projs = specialProjectList.get(specials[1]);
            String slp = "";
            String mimp = "";
            for(String project : internet1Projs) {
                slp = streamLogPath + project + "/";
                mimp = mysqlIdMapPath + "vf_" + project + "/id_map.txt";
                FileInputFormat.addInputPaths(activeJob, slp);
                FileInputFormat.addInputPaths(activeJob, mimp);
                slp = "";
                mimp = "";
            }



//            FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);


            activeJob.setInputFormatClass(TextInputFormat.class);
            activeJob.setMapperClass(ActiveMapper.class);
            activeJob.setMapOutputKeyClass(Text.class);
            activeJob.setMapOutputValueClass(JoinData.class);

            activeJob.setReducerClass(ActiveReducer.class);
            activeJob.setNumReduceTasks(3);
            activeJob.setOutputKeyClass(Text.class);
            activeJob.setOutputValueClass(NullWritable.class);
            FileOutputFormat.setOutputPath(activeJob, new Path(outputPath));
            FileOutputFormat.setCompressOutput(activeJob, true);
            FileOutputFormat.setOutputCompressorClass(activeJob, GzipCodec.class);
            activeJob.setOutputFormatClass(TextOutputFormat.class);


            activeJob.waitForCompletion(true);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public String getYesterday(int type) {
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

    public static Map<String, List<String>> getSpecialProjectList() throws Exception {
        Map<String, List<String>> projectList = new HashMap<String, List<String>>();
        File file = new File("/home/hadoop/ba/BA/conf/specialtask");
        String json = "";
        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line = null;
            while ((line = reader.readLine()) != null) {
                json += line;
            }
            JSONArray jsonArray = JSONArray.fromObject(json);
            for (Object object : jsonArray) {
                JSONObject jsonObj = (JSONObject) object;
                String project = jsonObj.getString("project");
                String[] projects = jsonObj.getString("members").split(",");
                List<String> memberList = new ArrayList<String>();
                for (String member : projects) {
                    String kv[] = member.split(":");
                    memberList.add(kv[0]);
                }
                projectList.put(project, memberList);
            }
        } catch (Exception e) {
            throw new Exception("parse the json(/home/hadoop/ba/BA/conf/specialtask) " + json + " get exception "  + e.getMessage());
        }
        return projectList;
    }
}
