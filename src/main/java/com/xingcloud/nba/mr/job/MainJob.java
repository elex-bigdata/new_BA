package com.xingcloud.nba.mr.job;

import com.xingcloud.nba.utils.FileManager;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;

/**
 * Created by wanghaixing on 14-7-31.
 */
public class MainJob {
    private static Log LOG = LogFactory.getLog(MainJob.class);
    private static String allPath = "hdfs://ELEX-LA-WEB1:19000/user/hadoop/";

    public static void main(String[] args) {
        try {

            MainJob mainJob = new MainJob();

            String[] specials = {"internet","internet-2"};   //"internet", "internet-1", "internet-2"
            Map<String, List<String>> specialProjectList = getSpecialProjectList();

            mainJob.runActiveJob(specials, specialProjectList);

            //对生成的UID进行处理：去重，统计
//            mainJob.runAnalyzeJob(specials);
//            new Thread(new AnalyzeJob("internet-1")).start();

//            Thread.sleep(60000);
//            FileManager.deleteFile();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public int runActiveJob(String[] specials, Map<String, List<String>> specialProjectList) {
        try {
            List<String> projects = new ArrayList<String>();
            Thread[] task = new Thread[3];
            int i = 0;
            for(String specialTask : specials) {
                projects = specialProjectList.get(specialTask);
                Runnable r = new ActiveJob(specialTask, projects);
                task[i] = new Thread(r);
                task[i].start();
                i += 1;
            }
            //等待生成所有UID完成
            for(Thread t : task) {
                t.join();
            }
            return 0;
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("runActiveJob job got exception!", e);
            return -1;
        }

    }

    public int runAnalyzeJob(String[] specials) {
        try {
            Thread[] task = new Thread[3];
            int i = 0;
            for(String specialTask : specials) {
                Runnable r = new AnalyzeJob(specialTask);
                task[i] = new Thread(r);
                task[i].start();
                i += 1;
            }
            System.out.println(task.length);
            for(Thread t : task) {
                if(t == null) System.out.println("***********************************");
                t.join();
            }
            return 0;
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("runAnalyzeJob job got exception!", e);
            return -1;
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
