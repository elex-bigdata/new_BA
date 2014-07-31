package com.xingcloud.nba.mr.job;

import com.xingcloud.nba.mr.inputformat.MyCombineFileInputFormat;
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
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by wanghaixing on 14-7-31.
 */
public class MainJob {
    private static Log LOG = LogFactory.getLog(ActiveJob.class);

    public static void main(String[] args) {
        try {

            String[] specials = {"internet-1"};   //"internet", "internet-1", "internet-2"
            Map<String, List<String>> specialProjectList = getSpecialProjectList();
            List<String> projects = new ArrayList<String>();

            for(String specialTask : specials) {
                projects = specialProjectList.get(specialTask);
                Runnable r = new ActiveJob(specialTask, projects);
                new Thread(r).start();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

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
