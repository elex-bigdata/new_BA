package com.xingcloud.nba.service;

import com.xingcloud.nba.task.BASQLGenerator;
import com.xingcloud.nba.hive.HiveJdbcClient;
import com.xingcloud.nba.task.InternetDAO;
import com.xingcloud.nba.task.PlainSQLExcecutor;
import com.xingcloud.nba.utils.Constant;
import com.xingcloud.nba.utils.DateManager;

import java.sql.*;
import java.text.ParseException;
import java.util.*;
import java.util.Date;
import java.util.concurrent.*;

/**
 * Author: liqiang
 * Date: 14-8-26
 * Time: 下午1:58
 */
public class BAService {

    public InternetDAO dao = new InternetDAO();

    public void dailyJob(Map<String,List<String>> projects, String day) throws Exception {

        //alter
        dao.alterTable(projects.get(Constant.INTERNET), day);

        //trans
        transProjectUID(projects,day);

        //au
        Map<String,String> auKV = calActiveUser(projects.keySet(),day);

        //new
        Map<String,String> newUserKV = calNewUser(projects.keySet(), day);


    }


    /**
     * 转化Internet1,Internet2中Visit事件、注册时间、geoip的内部UID为原始UID（orig_id）
     * @param tasks
     * @param day
     * @throws SQLException
     * @throws ParseException
     */
    public void transProjectUID(Map<String,List<String>> tasks, String day) throws SQLException, ParseException {

        String transInt1VisitSQL = BASQLGenerator.getTransVistUIDSql(Constant.INTERNET1, tasks.get(Constant.INTERNET1), day);
        String transInt2VisitSQL = BASQLGenerator.getTransVistUIDSql(Constant.INTERNET2, tasks.get(Constant.INTERNET2), day);

        String transInt1RegSQL = BASQLGenerator.getTransRegisterTimeUIDSql(Constant.INTERNET1, tasks.get(Constant.INTERNET1));
        String transInt2RegSQL = BASQLGenerator.getTransRegisterTimeUIDSql(Constant.INTERNET2, tasks.get(Constant.INTERNET2));

        String transInt1GeoipSQL = BASQLGenerator.getTransGeoIPUIDSql(Constant.INTERNET1, tasks.get(Constant.INTERNET1));
        String transInt2GeoipSQL = BASQLGenerator.getTransGeoIPUIDSql(Constant.INTERNET2, tasks.get(Constant.INTERNET2));

        ExecutorService service = Executors.newFixedThreadPool(6);

        List<Future<String>> results = new ArrayList<Future<String>>();
        long beginTime = System.nanoTime();
        results.add(service.submit(new PlainSQLExcecutor(transInt1VisitSQL)));
        results.add(service.submit(new PlainSQLExcecutor(transInt2VisitSQL)));
        results.add(service.submit(new PlainSQLExcecutor(transInt1RegSQL)));
        results.add(service.submit(new PlainSQLExcecutor(transInt2RegSQL)));
        results.add(service.submit(new PlainSQLExcecutor(transInt1GeoipSQL)));
        results.add(service.submit(new PlainSQLExcecutor(transInt2GeoipSQL)));


        for(Future<String> result: results){
            try {
                result.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        service.shutdown();
        System.out.println("Trans all uid spend " + (System.nanoTime() - beginTime)/1.0e9 + " seconds");
    }

    //k: redis key, value: redis value
    public Map<String,String> calActiveUser(Set<String> projects, String day) throws Exception {

        Map<String,String> kv = new HashMap<String,String>();

        Date date = DateManager.dayfmt.parse(day);
        for(String project : projects){

            String[] pids = new String[]{project};
            if(Constant.INTERNET.equals(project)){
                pids = new String[]{Constant.INTERNET1,Constant.INTERNET2};
            }

            //日
            String[] days = new String[]{day};
            long dau = dao.countActiveUser(days, pids); 
            //kv.put(key,value)

            //周
            days = new String[]{DateManager.getDaysBefore(date,7,0),DateManager.getDaysBefore(date,1,0)};
            long wau = dao.countActiveUser(days, pids);
            //kv.put(key,value)

            days = new String[]{DateManager.getDaysBefore(date,30,0),DateManager.getDaysBefore(date,1,0)};
            long mau = dao.countActiveUser(days, pids);
            //kv.put(key,value)

        }


        return kv;
    }

    //k: redis key, value: redis value
    public Map<String,String> calNewUser(Set<String> projects, String day) throws Exception {

        Map<String,String> kv = new HashMap<String,String>();

        for(String project : projects){

            String[] pids = new String[]{project};
            if(Constant.INTERNET.equals(project)){
                pids = new String[]{Constant.INTERNET1,Constant.INTERNET2};
            }

            long nu = dao.countNewUser(day, pids);
            //kv.put(key,value)
        }
        return kv;
    }

    //k: redis key, value: redis value
    public Map<String,String> calRetainUser(Set<String> projects, String day) throws Exception {

        Map<String,String> kv = new HashMap<String,String>();

        Date date = DateManager.dayfmt.parse(day);
        for(String project : projects){

            String[] pids = new String[]{project};
            if(Constant.INTERNET.equals(project)){
                pids = new String[]{Constant.INTERNET1,Constant.INTERNET2};
            }

            //2日
            String[] days = new String[]{day};
            long tru = dao.countRetentionUser(DateManager.getDaysBefore(date, 1, 0), days, pids);
            //kv.put(key,value)

            //7日
            long sru = dao.countRetentionUser(DateManager.getDaysBefore(date, 6, 0), days, pids);
            //kv.put(key,value)

            //一周
            days = new String[]{DateManager.getDaysBefore(date, 6, 0),day};
            long wru = dao.countRetentionUser(DateManager.getDaysBefore(date, 7, 0), days, pids);
            //kv.put(key,value)
        }
        return kv;
    }

}

