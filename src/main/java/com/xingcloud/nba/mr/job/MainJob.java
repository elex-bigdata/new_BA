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

            String[] specials = {"internet-1", "internet-2"};   //"internet", "internet-1", "internet-2"
            Map<String, List<String>> specialProjectList = getSpecialProjectList();

//            mainJob.runActiveJob(specials, specialProjectList);

            //对生成的UID进行处理：去重，统计
//            mainJob.runAnalyzeJob(specials);

//            Thread.sleep(60000);
//            FileManager.deleteFile();



            List<String> projects = new ArrayList<String>();


            /*for(String specialTask : specials) {
                projects = specialProjectList.get(specialTask);
                Runnable r = new ActiveJob(specialTask, projects);
                task[i] = new Thread(r);
                task[i].start();
                i += 1;
            }
            //等待生成所有UID完成
            for(Thread t : task) {
                t.join();
            }*/


            /*i = 0;
            for(String specialTask : specials) {
                Runnable r = new AnalyzeJob(specialTask);
                task[i] = new Thread(r);
                task[i].start();
                i += 1;
            }*/


            /*projects = specialProjectList.get("internet-1");
            int len = projects.size();
            int i = 0;
            Thread[] task = new Thread[len];
            for(String project : projects) {
                Runnable r = new ProjectJob("internet-1", project);
                task[i] = new Thread(r);
                task[i].start();
                i += 1;
            }*/


//            new Thread(new AnalyzeJob("internet-1", projects)).start();


            int ret = mainJob.runProjectJob(specials, specialProjectList);

            if(ret == 0) {
                mainJob.runAnalyzeJob(specials, specialProjectList);
            }

            System.out.println("the raw uid all generated................");


        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public int runProjectJob(String[] specials, Map<String, List<String>> specialProjectList) {
        try {
            int projectNum = 0;
            for(String specialTask : specials) {
                List<String> projects = specialProjectList.get(specialTask);
                projectNum += projects.size();
            }
            Thread[] task = new Thread[projectNum];

            for(String specialTask : specials) {
                List<String> projects = specialProjectList.get(specialTask);
                int i = 0;
                for(String project : projects) {
                    Runnable r = new ProjectJob(specialTask, project);
                    task[i] = new Thread(r);
                    task[i].start();
                    i += 1;
                }

            }
            for(Thread t : task) {
                if(t != null) {
                    t.join();
                }
            }

            return 0;
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("runProjectJob job got exception!", e);
            return -1;
        }
    }

    public int runAnalyzeJob(String[] specials, Map<String, List<String>> specialProjectList) {
        try {
            int len = specials.length;
            Thread[] task = new Thread[len];
            int i = 0;
            for(String specialTask : specials) {
                List<String> projects = specialProjectList.get(specialTask);
                Runnable r = new AnalyzeJob(specialTask, projects);
                task[i] = new Thread(r);
                task[i].start();
                i += 1;
            }
            for(Thread t : task) {
                if(t != null) {
                    t.join();
                }
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
