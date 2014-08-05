package com.xingcloud.nba.mr.job;

import com.xingcloud.nba.business.StoreResult;
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


            /*int ret = mainJob.runProjectJob(specials, specialProjectList);

            if(ret == 0) {
                mainJob.runAnalyzeJob(specials, specialProjectList);
            }*/

//            mainJob.runInternetJob();
//            LOG.info("the raw uid all generated................");

            /*ActiveJob r = new ActiveJob("internet-1", 3);
            Thread t = new Thread(r);
            t.start();
            t.join();
            long l = r.getCount();
            System.out.println(l);
            new StoreResult().store(l);*/

            long[][] activeCounts = new long[3][3];
            mainJob.runActiveJob("internet-2", activeCounts[0]);
            for(long l : activeCounts[0])
                System.out.println(l);

            new StoreResult().store(activeCounts[0]);

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

    public void runInternetJob() {
        InternetJob job = new InternetJob();
        job.run();
    }

    public void runActiveJob(String specialTask, long[] counts) {
        Thread[] task = new Thread[3];
        Runnable[] aj = new Runnable[3];
        try {
            for(int i = 0; i < 3; i++) {
                aj[i] = new ActiveJob(specialTask, i + 1);
                task[i] = new Thread(aj[i]);
                task[i].start();
            }
            for(int i = 0; i < 3; i++) {
                if(task[i] != null) {
                    task[i].join();
                    counts[i] = ((ActiveJob)aj[i]).getCount();
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("runActiveJob job got exception!", e);
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
