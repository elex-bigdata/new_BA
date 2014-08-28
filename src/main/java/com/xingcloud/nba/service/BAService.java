package com.xingcloud.nba.service;

import com.xingcloud.maincache.MapXCache;
import com.xingcloud.maincache.XCacheException;
import com.xingcloud.maincache.XCacheOperator;
import com.xingcloud.maincache.redis.RedisXCacheOperator;
import com.xingcloud.nba.task.BASQLGenerator;
import com.xingcloud.nba.hive.HiveJdbcClient;
import com.xingcloud.nba.task.InternetDAO;
import com.xingcloud.nba.task.PlainSQLExcecutor;
import com.xingcloud.nba.utils.Constant;
import com.xingcloud.nba.utils.DateManager;

import java.io.*;
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
        Map<String, Long> auKV = calActiveUser(projects.keySet(),day);

        //new
        Map<String, Long> newUserKV = calNewUser(projects.keySet(), day);

        //geoip

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
        /*results.add(service.submit(new PlainSQLExcecutor(transInt1VisitSQL)));
        results.add(service.submit(new PlainSQLExcecutor(transInt2VisitSQL)));
        results.add(service.submit(new PlainSQLExcecutor(transInt1RegSQL)));
        results.add(service.submit(new PlainSQLExcecutor(transInt2RegSQL)));
        results.add(service.submit(new PlainSQLExcecutor(transInt1GeoipSQL)));
        results.add(service.submit(new PlainSQLExcecutor(transInt2GeoipSQL)));*/


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
    public Map<String, Long> calActiveUser(Set<String> projects, String day) throws Exception {

        Map<String, Long> kv = new HashMap<String, Long>();

        Date date = DateManager.dayfmt.parse(day);
        String visitDate = DateManager.getDaysBefore(date, 0, 0);
        for(String project : projects){

            //日
            String[] days = new String[]{day};
            long dau = dao.countActiveUser(days, project);
            String dauKey = "COMMON," + project + "," + visitDate + "," + visitDate + ",visit.*,TOTAL_USER,VF-ALL-0-0,PERIOD";
            kv.put(dauKey, dau);

            //周
            String beginDate = DateManager.getDaysBefore(date,7,0);
            String endDate = DateManager.getDaysBefore(date,0,0);
            days = new String[]{beginDate,endDate};
            long wau = dao.countActiveUser(days, project);
            String wauKey = "COMMON," + project + "," + beginDate + "," + endDate + ",visit.*,TOTAL_USER,VF-ALL-0-0,PERIOD";
            kv.put(wauKey, wau);

            //月
            beginDate = DateManager.getDaysBefore(date,30,0);
            endDate = DateManager.getDaysBefore(date,0,0);
            days = new String[]{beginDate,endDate};
            long mau = dao.countActiveUser(days, project);
            String mauKey = "COMMON," + project + "," + beginDate + "," + endDate + ",visit.*,TOTAL_USER,VF-ALL-0-0,PERIOD";
            kv.put(mauKey, mau);

        }

        return kv;
    }

    //k: redis key, value: redis value
    public Map<String, Long> calNewUser(Set<String> projects, String day) throws Exception {

        Date date = DateManager.dayfmt.parse(day);
        String visitDate = DateManager.getDaysBefore(date, 0, 0);
        Map<String, Long> kv = new HashMap<String, Long>();

        for(String project : projects){

            long nu = dao.countNewUser(day, project);
            String nuKey = "COMMON," + project + "," + visitDate + "," + visitDate + ",visit.*,{\"register_time\":{\"$gte\":\"" + visitDate + "\",\"$lte\":\"" + visitDate + "\"}},VF-ALL-0-0,PERIOD";
            kv.put(nuKey, nu);
        }
        return kv;
    }

    //k: redis key, value: redis value
    public Map<String, Long> calRetainUser(Set<String> projects, String day) throws Exception {

        Date date = DateManager.dayfmt.parse(day);
        String visitDate = DateManager.getDaysBefore(date, 0, 0);
        Map<String, Long> kv = new HashMap<String, Long>();

        for(String project : projects){

            //2日
            String[] days = new String[]{day};
            String regDate = DateManager.getDaysBefore(date, 1, 0);
            long tru = dao.countRetentionUser(regDate, days, project);
            String truKey = "COMMON," + project + "," + visitDate + "," + visitDate + ",visit.*,{\"register_time\":{\"$gte\":\"" + regDate + "\",\"$lte\":\"" + regDate + "\"}},VF-ALL-0-0,PERIOD";
            kv.put(truKey, tru);

            //7日
            regDate = DateManager.getDaysBefore(date, 6, 0);
            long sru = dao.countRetentionUser(regDate, days, project);
            String sruKey = "COMMON," + project + "," + visitDate + "," + visitDate + ",visit.*,{\"register_time\":{\"$gte\":\"" + regDate + "\",\"$lte\":\"" + regDate + "\"}},VF-ALL-0-0,PERIOD";
            kv.put(sruKey, sru);

            //一周
            String beginDate = DateManager.getDaysBefore(date,6,0);
            String endDate = DateManager.getDaysBefore(date,0,0);;
            regDate = DateManager.getDaysBefore(date, 7, 0);
            days = new String[]{DateManager.getDaysBefore(date, 6, 0),day};
            long wru = dao.countRetentionUser(regDate, days, project);
            String wruKey = "COMMON," + project + "," + beginDate + "," + endDate + ",visit.*,{\"register_time\":{\"$gte\":\"" + regDate + "\",\"$lte\":\"" + regDate + "\"}},VF-ALL-0-0,PERIOD";
            kv.put(wruKey, wru);
        }
        return kv;
    }

    //COMMON,age,2014-08-25,2014-08-25,visit.*,{"geoip":"ua","register_time":{"$gte":"2014-08-25","$lte":"2014-08-25"}},VF-ALL-0-0,PERIOD

    //GROUP,age,2014-08-25,2014-08-25,visit.*,{"register_time":{"$gte":"2014-08-25","$lte":"2014-08-25"}},VF-ALL-0-0,USER_PROPERTIES,geoip
    public Map<String, Long> calNewUserByGeoip(Set<String> projects, String day) throws Exception {

        Date date = DateManager.dayfmt.parse(day);
        String visitDate = DateManager.getDaysBefore(date, 0, 0);

        Map<String,Long> kv = new HashMap<String, Long>();
        Map<String,Long> result = new HashMap<String, Long>();
        String key = "";
        Number[] numbers = new Number[]{0, 0, 0, 1.0};
        Map<String, Number[]> geoipMap = new HashMap<String, Number[]>();

        for(String project : projects) {
            result = dao.countNewUserByGeoip(project, day);
            for(Map.Entry<String, Long> entry : result.entrySet()) {
                key = "COMMON," + project + "," + visitDate + "," + visitDate + ",visit.*,{\"geoip\":\"" + entry.getKey() + "\",\"register_time\":{\"$gte\":\"" + visitDate + "\",\"$lte\":\"" + visitDate + "\"}},VF-ALL-0-0,PERIOD";
                //common
                kv.put(key, entry.getValue());

                numbers[2] = entry.getValue();
                geoipMap.put(entry.getKey(), numbers);
            }
            //group
            key = "GROUP," + project + "," + visitDate + "," + visitDate + ",visit.*,{\"register_time\":{\"$gte\":\"" + visitDate + "\",\"$lte\":\"" + visitDate + "\"}},VF-ALL-0-0,USER_PROPERTIES,geoip";
            storeToRedisGroup(key, geoipMap);
        }

        return kv;
    }

    //COMMON,age,2014-08-26,2014-08-26,visit.*,{"geoip":"ua","register_time":{"$gte":"2014-08-25","$lte":"2014-08-25"}},VF-ALL-0-0,PERIOD

    //GROUP,age,2014-08-26,2014-08-26,visit.*,{"register_time":{"$gte":"2014-08-25","$lte":"2014-08-25"}},VF-ALL-0-0,USER_PROPERTIES,geoip
    public Map<String, Long> calRetentionUserByGeoip(Set<String> projects, String day) throws Exception {
        Date date = DateManager.dayfmt.parse(day);
        String visitDate = DateManager.getDaysBefore(date, 0, 0);

        Map<String,Long> kv = new HashMap<String, Long>();
        Map<String,Long> result = new HashMap<String, Long>();
        String key = "";
        Number[] numbers = new Number[]{0, 0, 0, 1.0};
        Map<String, Number[]> geoipMap = new HashMap<String, Number[]>();

        String regDate = DateManager.getDaysBefore(date, 1, 0);

        for(String project : projects) {
            //2日
            result = dao.countRetentionUserByGeoip(regDate, new String[]{visitDate}, project);
            for(Map.Entry<String, Long> entry : result.entrySet()) {
                key = "COMMON," + project + "," + visitDate + "," + visitDate + ",visit.*,{\"geoip\":\"" + entry.getKey() + "\",\"register_time\":{\"$gte\":\"" + regDate + "\",\"$lte\":\"" + regDate + "\"}},VF-ALL-0-0,PERIOD";
                //common
                kv.put(key, entry.getValue());

                numbers[2] = entry.getValue();
                geoipMap.put(entry.getKey(), numbers);
            }
            //group
            key = "GROUP," + project + "," + visitDate + "," + visitDate + ",visit.*,{\"register_time\":{\"$gte\":\"" + regDate + "\",\"$lte\":\"" + regDate + "\"}},VF-ALL-0-0,USER_PROPERTIES,geoip";
            storeToRedisGroup(key, geoipMap);
        }

        return kv;
    }

    public void storeToRedis(Map<String, Long> kv) {
        Map<String, Number[]> result = null;
        MapXCache xCache = null;
        XCacheOperator xCacheOperator = RedisXCacheOperator.getInstance();
        try {
            for(Map.Entry<String, Long> entry : kv.entrySet()) {
                result = new HashMap<String, Number[]>();
                result.put(entry.getKey(), new Number[]{0, 0, entry.getValue(), 1.0});
                xCache = MapXCache.buildMapXCache(entry.getKey(), result);
                xCacheOperator.putMapCache(xCache);
            }

        } catch (XCacheException e) {
            e.printStackTrace();
        }
    }

    public void storeToRedisGroup(String key, Map<String, Number[]> result) {
        MapXCache xCache = null;
        XCacheOperator xCacheOperator = RedisXCacheOperator.getInstance();
        try {
                xCache = MapXCache.buildMapXCache(key, result);
                xCacheOperator.putMapCache(xCache);
        } catch (XCacheException e) {
            e.printStackTrace();
        }
    }

    /**
     * store data to local file
     */
    public void storeToFile(String data) {

        String storeFilePath = "/home/hadoop/wanghaixing/storeData.txt";
        FileInputStream in = null;
        FileOutputStream out = null;
        try {
            in = new FileInputStream(new File(storeFilePath));
            InputStreamReader isReader = new InputStreamReader(in);
            BufferedReader fr = new BufferedReader(isReader);
            StringBuffer sb=new StringBuffer(data);
//            sb.append("2014-08-14" + "\t" +	"15380085" + "\t" +	"29118482" + "\t" +	"49172388" + "\t" +	"19846028" + "\t" +	"30768834" + "\t" + "45398978" + "\t" + "28805295" + "\t" + "45703594" + "\t" + "67068383" + "\r\n");
            String temp = "";
            int n = 0;
            while((temp = fr.readLine()) != null) {
                sb.append(temp);
                sb.append("\r\n");
                if(n++ == 89) { //保留3个月的数据
                    break;
                }
            }
            out = new FileOutputStream(new File(storeFilePath));
            out.write(sb.toString().getBytes("utf-8"));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                in.close();
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}

