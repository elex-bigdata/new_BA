package com.xingcloud.nba.main;

import com.xingcloud.nba.service.BAService;
import com.xingcloud.nba.service.ScanHBaseUID;
import com.xingcloud.nba.task.ServiceExcecutor;
import com.xingcloud.nba.task.Task;
import com.xingcloud.nba.utils.Constant;
import com.xingcloud.nba.utils.DateManager;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;
import java.util.concurrent.*;

/**
 * Author: liqiang
 * Date: 14-8-29
 * Time: 下午7:10
 */
public class OfflineCalculate {

    private static final Logger LOGGER = Logger.getLogger(OfflineCalculate.class);

    public static void main(String[] args) throws Exception {

        if(args.length != 2){
            System.out.println("Usage: (all|store) day");
            System.exit(-1);
        }

        String cmd = args[0];
        String day = args[1];

        BAService service = new BAService();

        if("all".equals(cmd)){
            Map<String, List<String>> specialProjectList = getSpecialProjectList();
            specialProjectList.remove("internet-3");
//            dailyJob(service, specialProjectList, day);
            testJob(service, specialProjectList, day);
        }else if("store".equals(cmd)){
            service.storeFromFile(day);
        }else{
            System.out.println("Unknown cmd,exit");
        }
    }

    public static void testJob(BAService service,Map<String,List<String>> projects, String day) throws Exception {
        Map<String, Map<String,Number[]>> allResult = new HashMap<String, Map<String,Number[]>>();
        //internet-1的搜索相关
        ScanHBaseUID shu = new ScanHBaseUID();
        String event = "pay.search2";
        String scanDay = DateManager.getDaysBefore(day, 1);
        allResult.putAll(shu.getResult(scanDay, event, projects.get(Constant.INTERNET1)));
        service.storeToRedis(allResult);
        service.cleanup();
    }


    public static void dailyJob(BAService service,Map<String,List<String>> projects, String day) throws Exception {

        long begin = System.currentTimeMillis();

        String[] attrs = new String[]{"geoip","ref0"};
        //alter
        service.alterTable(projects, day);
        //init partition
        service.initPartition(projects);
        //tran
        service.transProjectUID(projects, attrs, day);

        ExecutorService executor = new ThreadPoolExecutor(3,20,60, TimeUnit.MILLISECONDS,new LinkedBlockingQueue<Runnable>());

        //覆盖、细分目前只算internet-1
        Set<String> division = new HashSet<String>();
        division.add(Constant.INTERNET1);
        List<Future<Map<String, Map<String,Number[]>>>> results = new ArrayList<Future<Map<String, Map<String,Number[]>>>>();
        //细分
        for(String attr : attrs){
            results.add(executor.submit(new ServiceExcecutor(Task.ATTR_NEW, division, attr, day)));
            results.add(executor.submit(new ServiceExcecutor(Task.ATTR_RETAIN, division, attr, day)));
        }
        //覆盖
        results.add(executor.submit(new ServiceExcecutor(Task.COVER, division, day)));

        //活跃用户
        results.add(executor.submit(new ServiceExcecutor(Task.ACTIVE, projects.keySet(), day)));
        //新用户
        results.add(executor.submit(new ServiceExcecutor(Task.NEW, projects.keySet(), day)));
        //留存
        results.add(executor.submit(new ServiceExcecutor(Task.RETAIN, projects.keySet(), day)));

        Map<String, Map<String,Number[]>> allResult = new HashMap<String, Map<String,Number[]>>();
        for(Future<Map<String, Map<String,Number[]>>> result: results){
            try {
                allResult.putAll(result.get());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        executor.shutdown();

        //internet-1的搜索相关
        ScanHBaseUID shu = new ScanHBaseUID();
        String event = "pay.search2";
        String scanDay = DateManager.getDaysBefore(day, 1);
        allResult.putAll(shu.getResult(scanDay, event, projects.get(Constant.INTERNET1)));

        service.storeToFile(allResult, day, true);
        service.storeToRedis(allResult);
        service.cleanup();

        LOGGER.debug("Spend " + (System.currentTimeMillis() - begin) + " to execute " + day + " job");
    }
      
    public static Map<String, List<String>> getSpecialProjectList() throws Exception {
        Map<String, List<String>> projectList = new HashMap<String, List<String>>();
        File file = new File(Constant.SPECIAL_TASK_PATH);
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
            throw new Exception("parse the json("+Constant.SPECIAL_TASK_PATH+") " + json + " get exception "  + e.getMessage());
        }
        return projectList;
    }
}
