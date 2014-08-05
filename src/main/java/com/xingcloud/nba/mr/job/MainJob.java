package com.xingcloud.nba.mr.job;

import com.xingcloud.nba.business.StoreResult;
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

            List<String> specialList = new ArrayList<String>();
            specialList.add("internet-1");
            specialList.add("internet-2");
//            String[] specials = {"internet-1", "internet-2"};   //"internet", "internet-1", "internet-2"
            Map<String, List<String>> specialProjectList = getSpecialProjectList();


            int ret1 = mainJob.runProjectJob(specialList, specialProjectList);
            if(ret1 == 0) {
                mainJob.runAnalyzeJob(specialList, specialProjectList);
            }
            mainJob.runInternetJob();
            //所有的数据都生成完毕
            LOG.info("the raw uid all generated to /user/hadoop/offline/uid/................");

            long[][] activeCounts = new long[3][3];
            specialList.add("internet");
            for(int i = 0; i < 3; i++) {
                mainJob.runActiveJob(specialList.get(i), activeCounts[i]);
                //将统计好的活跃量放入redis中
                new StoreResult(specialList.get(i)).store(activeCounts[i]);
            }

            //将统计好的活跃量放入redis中
//            new StoreResult("internet").store(activeCounts[0]);


            /*ActiveJob r = new ActiveJob("internet-1", 3);
            Thread t = new Thread(r);
            t.start();
            t.join();
            long l = r.getCount();
            System.out.println(l);
            new StoreResult().store(l);*/



        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 分别将internet-1、internet-2中每个项目前一天的stream_log和对应的mysqlidmap进行连接操作获得原始uid
     * 每个项目一个job，多线程运行
     * @param specials internet-1等
     * @param specialProjectList 项目列表
     * @return
     */
    public int runProjectJob(List<String> specials, Map<String, List<String>> specialProjectList) {
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
                    t.join();       //等待这些job运行完成，进行后续操作
                }
            }

            return 0;
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("runProjectJob job got exception!", e);
            return -1;
        }
    }

    /**
     * 分别对internet-1、internet-2中的前一天的所有项目的原始uid进行去重，结果放到/user/hadoop/offline/uid/中
     * @param specials internet-1等
     * @param specialProjectList 项目列表
     * @return
     */
    public int runAnalyzeJob(List<String> specials, Map<String, List<String>> specialProjectList) {
        try {
            int len = specials.size();
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

    /**
     * 对internet-1和internet-2中的前一天的所有项目原始uid一起去重，生成internet里的原始uid,
     * 因为internet-1和internet-2中的项目加起来是internet中的项目
     */
    public void runInternetJob() {
        try {
            Thread t = new Thread(new InternetJob());
            t.start();
            t.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 分别统计internet、internet-1和internet-2中的日活跃量、周活跃量、月活跃量
     * @param specialTask
     * @param counts
     */
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
