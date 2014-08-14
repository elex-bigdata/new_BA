package com.xingcloud.nba.mr.job;

import com.xingcloud.nba.business.StoreResult;
import com.xingcloud.nba.utils.Constant;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.DecimalFormat;
import java.util.*;

/**
 * Created by wanghaixing on 14-7-31.
 */
public class MainJob {
    private static Log LOG = LogFactory.getLog(MainJob.class);
    private static String allPath = "hdfs://ELEX-LA-WEB1:19000/";
    private static String storeFilePath = "/user/whx/storeData";

    public static void main(String[] args) {
        try {

            MainJob mainJob = new MainJob();
            List<String> specialList = new ArrayList<String>();
            specialList.add("internet-1");
            specialList.add("internet-2");
            Map<String, List<String>> specialProjectList = getSpecialProjectList();

            /*int ret1 = mainJob.runProjectJob(specialList, specialProjectList);
            if(ret1 == 0) {
                mainJob.runAnalyzeJob(specialList, specialProjectList);
            }
            mainJob.runInternetJob(Constant.ACT_UNIQ);
            //所有的数据都生成完毕
            LOG.info("the raw uids all generated to /user/hadoop/offline/uid/................");*/

//------------------------------------------------------------------------------------------------------

//            mainJob.runRegUidJob(specialList, specialProjectList);
            /*long[] newCounts = new long[3];
            newCounts = mainJob.runBeUiniqJob(specialList, specialProjectList);
            newCounts[2] = mainJob.runInternetJob(Constant.NEW_UNIQ);
            LOG.info("the raw uids registerd a week ago have generated......");
            for(long l : newCounts) {
                System.out.println(l);
            }*/

            /*specialList.add("internet");
            for(int i = 0; i < 3; i++) {
                new StoreResult(specialList.get(i)).storeNewUserNum(newCounts[i]);
            }*/

            /*if((mainJob.runTransUidJob(specialList, specialProjectList) == 0)) {
                mainJob.runRegUidJob(specialList, specialProjectList);
            }*/
            specialList.add("internet");
            long[] newCounts = new long[3];
            /*newCounts = mainJob.runCalNewUserJob(specialList);
            for(long l : newCounts) {
                System.out.println(l);
            }*/
            newCounts[0] = 377511;
            newCounts[1] = 377136;
            newCounts[2] = 439181;
            for(int i = 0; i < 3; i++) {
                new StoreResult(specialList.get(i)).storeNewUserNum(newCounts[i]);
            }







            /*List<String> projects = specialProjectList.get("internet-1");
            BeUiniqJob bj = new BeUiniqJob("internet-1", projects, 0);
            new Thread(bj).start();
            long rb = bj.getCount();*/



            /*RetentionJob rj = new RetentionJob("internet-1");
            rj.run();
            long c = rj.getCount();
            System.out.println(c);*/

//------------------------------------------------------------------------------------------------------

//            new StoreResult("internet-1").storeNewUserNum(460168L);

            /*long[][] activeCounts = new long[3][3];
            specialList.add("internet");
            for(int i = 0; i < 3; i++) {
                mainJob.runActiveJob(specialList.get(i), activeCounts[i]);
                //将统计好的活跃量放入redis中
                new StoreResult(specialList.get(i)).storeActive(activeCounts[i]);
            }*/

            //手动将活跃量写入redis
            /*long[][] activeCounts = new long[3][3];
            activeCounts[0][0] = 14610509;
            activeCounts[0][1] = 28809239;
            activeCounts[0][2] = 49311640;
            activeCounts[1][0] = 20287744;
            activeCounts[1][1] = 31060351;
            activeCounts[1][2] = 44274626;
            activeCounts[2][0] = 28268584;
            activeCounts[2][1] = 45357607;
            activeCounts[2][2] = 66293212;
            for(int i = 0; i < 3; i++) {
                //将统计好的活跃量放入redis中
                new StoreResult(specialList.get(i)).storeActive(activeCounts[i]);
            }*/

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
    public long runInternetJob(int type) {
        try {
            Runnable r = new InternetJob(type);
            Thread t = new Thread(r);
            t.start();
            t.join();
            return ((InternetJob)r).getCount();
        } catch (InterruptedException e) {
            e.printStackTrace();
            return 0;
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

    public int runTransUidJob(List<String> specials, Map<String, List<String>> specialProjectList) {
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
                    Runnable r = new TransUidJob(specialTask, project);
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
     * 分别将internet-1、internet-2中每个项目前一天的stream_log和对应的mysqlidmap进行连接操作获得原始uid
     * 每个项目一个job，多线程运行
     * @param specials
     * @param specialProjectList
     * @return
     */
    /*public static int runRegUidJob(List<String> specials, Map<String, List<String>> specialProjectList) {
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
                    Runnable r = new RegUidJob(specialTask, project);
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
    }*/

    public void runRegUidJob(List<String> specials, Map<String, List<String>> specialProjectList) {
        int len = specials.size();
        Thread[] task = new Thread[len];
        Runnable[] bj = new Runnable[len];
        try {
            for(int i = 0; i < len; i++) {
                String specialTask = specials.get(i);
                List<String> projects = specialProjectList.get(specialTask);
                bj[i] = new RegUidJob(specialTask, projects);
                task[i] = new Thread(bj[i]);
                task[i].start();
            }
            for(int i = 0; i < len; i++) {
                if(task[i] != null) {
                    task[i].join();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("runRegUidJob job got exception!", e);
        }
    }

    public long[] runCalNewUserJob(List<String> specials) {
        long[] newCounts = new long[3];
        int len = specials.size();
        Thread[] task = new Thread[len];
        Runnable[] cnu = new Runnable[len];
        try {
            for(int i = 0; i < len; i++) {
                String specialTask = specials.get(i);
                if(specialTask.equals("internet")) {
                    cnu[i] = new CalcInternetJob();
                    task[i] = new Thread(cnu[i]);
                    task[i].start();
                } else {
                    cnu[i] = new CalcNewUserJob(specialTask);
                    task[i] = new Thread(cnu[i]);
                    task[i].start();
                }
            }
            for(int i = 0; i < len; i++) {
                if(task[i] != null) {
                    task[i].join();
                    if(i == 2) {
                        newCounts[i] = ((CalcInternetJob)cnu[i]).getCount();
                    } else {
                        newCounts[i] = ((CalcNewUserJob)cnu[i]).getCount();
                    }
                }
            }
            return  newCounts;
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("runRegUidJob job got exception!", e);
            return null;
        }
    }

    public static long[] runBeUiniqJob(List<String> specials, Map<String, List<String>> specialProjectList) {
        long[] uniqCounts = new long[3];
        int len = specials.size();
        Thread[] task = new Thread[len];
        Runnable[] bj = new Runnable[len];
        try {
            for(int i = 0; i < len; i++) {
                String specialTask = specials.get(i);
                List<String> projects = specialProjectList.get(specialTask);
                bj[i] = new BeUiniqJob(specialTask, projects, Constant.DAY_UNIQ);
                task[i] = new Thread(bj[i]);
                task[i].start();
            }
            for(int i = 0; i < len; i++) {
                if(task[i] != null) {
                    task[i].join();
                    uniqCounts[i] = ((BeUiniqJob)bj[i]).getCount();
                }
            }
            return uniqCounts;
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("runBeUiniqJob job got exception!", e);
            return null;
        }

    }

    public static long[] runBeUiniqJob2(List<String> specials, Map<String, List<String>> specialProjectList) {
        long[] uniqCounts = new long[3];
        int len = specials.size();
        List<String> projects = new ArrayList<String>();
        try {
            for(int i = 0; i < len; i++) {
                String specialTask = specials.get(i);
                List<String> t = specialProjectList.get(specialTask);
                projects.addAll(t);
            }
            Runnable bj = new BeUiniqJob2("internet", projects, Constant.DAY_UNIQ);
            Thread task = new Thread(bj);
            task.start();

            return uniqCounts;
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("runBeUiniqJob job got exception!", e);
            return null;
        }

    }

    public static double runRetentionJob(List<String> specials) {
        for(String specialTask : specials) {

        }

        long regNum = 0L;
        long weekNum = 0L;

        return 0;
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

    public void storeToHdfs() throws Exception{
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(allPath), conf);

        FSDataInputStream in = fileSystem.open(new Path(storeFilePath));
        IOUtils.copyBytes(in, System.out, 1024, true);


        if(fileSystem.exists(new Path(storeFilePath))) {
            fileSystem.delete(new Path(storeFilePath), true);
        }

    }

}
