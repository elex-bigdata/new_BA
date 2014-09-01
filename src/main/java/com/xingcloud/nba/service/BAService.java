package com.xingcloud.nba.service;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.xingcloud.maincache.MapXCache;
import com.xingcloud.maincache.XCacheException;
import com.xingcloud.maincache.XCacheOperator;
import com.xingcloud.maincache.redis.RedisXCacheOperator;
import com.xingcloud.nba.task.BASQLGenerator;
import com.xingcloud.nba.task.InternetDAO;
import com.xingcloud.nba.task.PlainSQLExcecutor;
import com.xingcloud.nba.utils.BAUtil;
import com.xingcloud.nba.utils.Constant;
import com.xingcloud.nba.utils.DateManager;

import java.io.*;
import java.lang.reflect.Type;
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

        String[] attrs = new String[]{"nation","geoip","ref0"};
        //alter
        dao.alterTable(projects.get(Constant.INTERNET), day);
        //trans
        transProjectUID(projects,attrs,day);

        //活跃用户
        Map<String, Map<String,Number[]>> auKV = calActiveUser(projects.keySet(),day);
        //new
        Map<String, Map<String,Number[]>> newUserKV = calNewUser(projects.keySet(), day);
        Map<String, Map<String,Number[]>> retainKv = calRetainUser(projects.keySet(), day);

        //只算internet-1
        Set<String> division = new HashSet<String>();
        division.add(Constant.INTERNET1);

        //覆盖
        Map<String, Map<String,Number[]>> coverKv = calNewCoverUser(division, day);

        Map<String, Map<String,Number[]>> newUserAttrKV = calNewUserByAttr(division, attrs, day);
        Map<String, Map<String,Number[]>> retentionAttrKV = calRetentionUserByAttr(division, attrs, day);

        Map<String, Map<String,Number[]>> allResult = new HashMap<String, Map<String,Number[]>>();
        allResult.putAll(auKV);
        allResult.putAll(newUserKV);
        allResult.putAll(retainKv);
        allResult.putAll(newUserAttrKV);
        allResult.putAll(retentionAttrKV);
        allResult.putAll(coverKv);

        storeToFile(allResult,day);
    }


    /**
     * 转化Internet1,Internet2中Visit事件、注册时间、geoip的内部UID为原始UID（orig_id）
     * @param tasks
     * @param day
     * @throws SQLException
     * @throws ParseException
     */
    public void transProjectUID(Map<String,List<String>> tasks, String[] attrs, String day) throws SQLException, ParseException {

        long beginTime = System.currentTimeMillis();
        //将streamlog转成原始UID去重放到user_visit各个分区
        String transInt1VisitSQL = BASQLGenerator.getTransVistUIDSql(Constant.INTERNET1, tasks.get(Constant.INTERNET1), day);
        String transInt2VisitSQL = BASQLGenerator.getTransVistUIDSql(Constant.INTERNET2, tasks.get(Constant.INTERNET2), day);
        String transIntVisitSQL = BASQLGenerator.getCombineVisitUIDSql(Constant.INTERNET, day,new String[]{Constant.INTERNET1, Constant.INTERNET2});
        String[] transVisit = new String[]{transInt1VisitSQL,transInt2VisitSQL,transIntVisitSQL};


        //将注册时间转成原始UID去重放到user_register_time各个分区
        String transInt1RegSQL = BASQLGenerator.getTransRegisterTimeUIDSql(Constant.INTERNET1, tasks.get(Constant.INTERNET1));
        String transInt2RegSQL = BASQLGenerator.getTransRegisterTimeUIDSql(Constant.INTERNET2, tasks.get(Constant.INTERNET2));
        String transIntRegSQL = BASQLGenerator.getCombineRegisterTimeUIDSql(Constant.INTERNET, new String[]{Constant.INTERNET1, Constant.INTERNET2});
        String[] transReg = new String[]{transInt1RegSQL,transInt2RegSQL,transIntRegSQL};

        //以下部分目前细分只考虑internet-1
        //将nation, geoip, ref0转成原始UID去重放到user_attribute各个分区

        String[] transAttrs = new String[attrs.length];
        for(int i=0;i<attrs.length;i++){
            transAttrs[i] = BASQLGenerator.getTransAttributeUIDSql(Constant.INTERNET1, attrs[i], tasks.get(Constant.INTERNET1));
        }

        ExecutorService service = Executors.newFixedThreadPool(3); //鉴于服务器压力，暂时只起3个线程，每个线程里的SQL顺序执行

        List<Future<String>> results = new ArrayList<Future<String>>();
        results.add(service.submit(new PlainSQLExcecutor(transVisit)));
        results.add(service.submit(new PlainSQLExcecutor(transReg)));
        results.add(service.submit(new PlainSQLExcecutor(transAttrs)));

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
        System.out.println("Trans all uid spend " + (System.currentTimeMillis() - beginTime) );
    }

    //k: redis key, value: redis value
    public Map<String, Map<String,Number[]>> calActiveUser(Set<String> projects, String day) throws Exception {

        Map<String, Map<String,Number[]>> kv = new HashMap<String, Map<String,Number[]>>();

        Date date = DateManager.dayfmt.parse(day);
        String visitDate = DateManager.getDaysBefore(date, 0, 0);
        String valueKey = visitDate + " 00:00";
        for(String project : projects){

            //日
            String[] days = new String[]{day};
            long dau = dao.countActiveUser(project, days);
            String dauKey = "COMMON," + project + "," + visitDate + "," + visitDate + ",visit.*,TOTAL_USER,VF-ALL-0-0,PERIOD";
            kv.put(dauKey,generateCacheValue(valueKey,dau));

            //周
            String beginDate = DateManager.getDaysBefore(date,7,0);
            String endDate = DateManager.getDaysBefore(date,0,0);
            days = new String[]{beginDate,endDate};
            long wau = dao.countActiveUser(project, days);
            String wauKey = "COMMON," + project + "," + beginDate + "," + endDate + ",visit.*,TOTAL_USER,VF-ALL-0-0,PERIOD";
            kv.put(wauKey,generateCacheValue(valueKey,wau));

            //月
            beginDate = DateManager.getDaysBefore(date,30,0);
            endDate = DateManager.getDaysBefore(date,0,0);
            days = new String[]{beginDate,endDate};
            long mau = dao.countActiveUser(project, days);
            String mauKey = "COMMON," + project + "," + beginDate + "," + endDate + ",visit.*,TOTAL_USER,VF-ALL-0-0,PERIOD";
            kv.put(mauKey,generateCacheValue(valueKey,mau));

        }

        return kv;
    }

    //k: redis key, value: redis value
    public Map<String, Map<String,Number[]>> calNewUser(Set<String> projects, String day) throws Exception {

        Date date = DateManager.dayfmt.parse(day);
        String visitDate = DateManager.getDaysBefore(date, 0, 0);
        Map<String, Map<String,Number[]>> kv = new HashMap<String, Map<String,Number[]>>();
        String valueKey = visitDate + " 00:00";
        for(String project : projects){

            long nu = dao.countNewUser(project, visitDate);
            String nuKey = "COMMON," + project + "," + visitDate + "," + visitDate + ",visit.*,{\"register_time\":{\"$gte\":\"" + visitDate + "\",\"$lte\":\"" + visitDate + "\"}},VF-ALL-0-0,PERIOD";
            kv.put(nuKey,generateCacheValue(valueKey,nu));
        }
        return kv;
    }

    //k: redis key, value: redis value
    public Map<String, Map<String,Number[]>> calRetainUser(Set<String> projects, String day) throws Exception {

        Date date = DateManager.dayfmt.parse(day);
        String visitDate = DateManager.getDaysBefore(date, 0, 0);
        Map<String, Map<String,Number[]>> kv = new HashMap<String, Map<String,Number[]>>();

        String valueKey = visitDate + " 00:00";
        for(String project : projects){

            //2日
            String[] days = new String[]{day};
            String regDate = DateManager.getDaysBefore(date, 1, 0);
            long tru = dao.countRetentionUser(project, regDate, days);
            String truKey = "COMMON," + project + "," + visitDate + "," + visitDate + ",visit.*,{\"register_time\":{\"$gte\":\"" + regDate + "\",\"$lte\":\"" + regDate + "\"}},VF-ALL-0-0,PERIOD";
            kv.put(truKey,generateCacheValue(valueKey,tru));


            //7日
            regDate = DateManager.getDaysBefore(date, 6, 0);
            long sru = dao.countRetentionUser(project, regDate, days);
            String sruKey = "COMMON," + project + "," + visitDate + "," + visitDate + ",visit.*,{\"register_time\":{\"$gte\":\"" + regDate + "\",\"$lte\":\"" + regDate + "\"}},VF-ALL-0-0,PERIOD";
            kv.put(sruKey,generateCacheValue(valueKey,sru));

            //一周
            String beginDate = DateManager.getDaysBefore(date,6,0);
            String endDate = DateManager.getDaysBefore(date,0,0);;
            regDate = DateManager.getDaysBefore(date, 7, 0);
            days = new String[]{DateManager.getDaysBefore(date, 6, 0),day};
            long wru = dao.countRetentionUser(project, regDate, days);
            String wruKey = "COMMON," + project + "," + beginDate + "," + endDate + ",visit.*,{\"register_time\":{\"$gte\":\"" + regDate + "\",\"$lte\":\"" + regDate + "\"}},VF-ALL-0-0,PERIOD";
            kv.put(wruKey,generateCacheValue(valueKey,wru));
        }
        return kv;
    }

    //COMMON,age,2014-08-25,2014-08-25,visit.*,{"geoip":"ua","register_time":{"$gte":"2014-08-25","$lte":"2014-08-25"}},VF-ALL-0-0,PERIOD

    //GROUP,age,2014-08-25,2014-08-25,visit.*,{"register_time":{"$gte":"2014-08-25","$lte":"2014-08-25"}},VF-ALL-0-0,USER_PROPERTIES,geoip
    public Map<String, Map<String,Number[]>> calNewUserByAttr(Set<String> projects, String[] attrs, String day) throws Exception {

        Date date = DateManager.dayfmt.parse(day);
        String visitDate = DateManager.getDaysBefore(date, 0, 0);

        String[] sevenDayRange = new String[2];
        sevenDayRange[0] = DateManager.getDaysBefore(date, 6, 0);
        sevenDayRange[1] = day;

        Map<String,Map<String,Number[]>> commonKV = new HashMap<String, Map<String,Number[]>>();
        Map<String,Long> result = null;
        String key = "";
        Number[] numbers = null;
        String valueKey = visitDate + " 00:00";
        for(String project : projects) {
            for(String attr:attrs){
                result = dao.countNewUserByAttr(project, attr, day);
                for(Map.Entry<String, Long> entry : result.entrySet()) {
                    key = "COMMON," + project + "," + visitDate + "," + visitDate + ",visit.*,{\""+attr+"\":\"" + entry.getKey() + "\",\"register_time\":{\"$gte\":\"" + visitDate + "\",\"$lte\":\"" + visitDate + "\"}},VF-ALL-0-0,PERIOD";
                    //common
                    commonKV.put(key,generateCacheValue(valueKey,entry.getValue()));
                }
                //group
                key = "GROUP," + project + "," + visitDate + "," + visitDate + ",visit.*,{\"register_time\":{\"$gte\":\"" + visitDate + "\",\"$lte\":\"" + visitDate + "\"}},VF-ALL-0-0,USER_PROPERTIES,"+attr;
                commonKV.put(key,generateCacheValue(result));

                //7day
                //GROUP,internet-1,2014-08-22,2014-08-28,visit.*,{"register_time":{"$gte":"2014-08-22","$lte":"2014-08-28"}},VF-ALL-0-0,USER_PROPERTIES,geoip
                result = dao.countNewUserByAttr(project, attr, sevenDayRange);
                key = "GROUP,"+project+","+sevenDayRange[0]+","+sevenDayRange[1]+",visit.*,{\"register_time\":{\"$gte\":\""+sevenDayRange[0]+"\",\"$lte\":\""+sevenDayRange[1]+"\"}},VF-ALL-0-0,USER_PROPERTIES,"+attr;
                commonKV.put(key,generateCacheValue(result));
            }
        }

        return commonKV;
    }

    //COMMON,age,2014-08-26,2014-08-26,visit.*,{"geoip":"ua","register_time":{"$gte":"2014-08-25","$lte":"2014-08-25"}},VF-ALL-0-0,PERIOD

    //GROUP,age,2014-08-26,2014-08-26,visit.*,{"register_time":{"$gte":"2014-08-25","$lte":"2014-08-25"}},VF-ALL-0-0,USER_PROPERTIES,geoip
    public Map<String, Map<String,Number[]>> calRetentionUserByAttr(Set<String> projects, String[] attrs, String day) throws Exception {
        Date date = DateManager.dayfmt.parse(day);
        String visitDate = DateManager.getDaysBefore(date, 0, 0);

        Map<String,Map<String,Number[]>> kv = new HashMap<String, Map<String,Number[]>>();
        Map<String,Long> result = null;
        String key = "";

        String day2 = DateManager.getDaysBefore(date, 1, 0);
        String day6 = DateManager.getDaysBefore(date, 6, 0);
        String day7 = DateManager.getDaysBefore(date, 7, 0);

        String[] singleDayRegDates = new String[]{day2,day6};

        for(String project : projects) {
            for(String attr: attrs){
                for(String regDate : singleDayRegDates){//2,7日
                    result = dao.countRetentionUserByAttr(project, attr, regDate, new String[]{visitDate});
                    for(Map.Entry<String, Long> entry : result.entrySet()) {
                        key = "COMMON," + project + "," + visitDate + "," + visitDate + ",visit.*,{\""+attr+"\":\"" + entry.getKey() + "\",\"register_time\":{\"$gte\":\"" + regDate + "\",\"$lte\":\"" + regDate + "\"}},VF-ALL-0-0,PERIOD";
                        //common
                        kv.put(key, generateCacheValue(regDate + " 00:00",entry.getValue()));
                    }
                    //group
                    key = "GROUP," + project + "," + visitDate + "," + visitDate + ",visit.*,{\"register_time\":{\"$gte\":\"" + regDate + "\",\"$lte\":\"" + regDate + "\"}},VF-ALL-0-0,USER_PROPERTIES,"+attr;
                    kv.put(key,generateCacheValue(result));
                }

                //一周
                result = dao.countRetentionUserByAttr(project, attr, day7, new String[]{day6,visitDate});
                for(Map.Entry<String, Long> entry : result.entrySet()) {
                    key = "COMMON," + project + "," + day6 + "," + visitDate + ",visit.*,{\""+attr+"\":\"" + entry.getKey() + "\",\"register_time\":{\"$gte\":\"" + day7 + "\",\"$lte\":\"" + day7 + "\"}},VF-ALL-0-0,PERIOD";
                    //common
                    kv.put(key, generateCacheValue(day7 + " 00:00",entry.getValue()));
                }
                //group
                key = "GROUP," + project + "," + day6 + "," + visitDate + ",visit.*,{\"register_time\":{\"$gte\":\"" + day7 + "\",\"$lte\":\"" + day7 + "\"}},VF-ALL-0-0,USER_PROPERTIES,"+attr;
                kv.put(key,generateCacheValue(result));

            }
        }

        return kv;
    }

    //internet-1覆盖 即当天注册的用户在其他项目已经注册过 int1cover:覆盖 int1totalnew: 覆盖加新增
    //COMMON,internet-1,2014-08-24,2014-08-24,int1totalnew.*,{"register_time":{"$gte":"2014-08-24","$lte":"2014-08-24"}},VF-ALL-0-0,PERIOD
    //COMMON,internet-1,2014-08-24,2014-08-24,int1cover.*,{"register_time":{"$gte":"2014-08-24","$lte":"2014-08-24"}},VF-ALL-0-0,PERIOD
    public Map<String, Map<String,Number[]>> calNewCoverUser(Set<String> projects, String day) throws Exception {
        Date date = DateManager.dayfmt.parse(day);
        String regDate = DateManager.getDaysBefore(date, 0, 0);

        Map<String,Map<String,Number[]>> kv = new HashMap<String, Map<String,Number[]>>();

        String valueKey = regDate + " 00:00";
        for(String project : projects){

            long ncu = dao.countNewCoverUser(project, regDate);
            String ncuKey = "COMMON," + project + "," + regDate + "," + regDate + ",int1cover.*,{\"register_time\":{\"$gte\":\"" + regDate + "\",\"$lte\":\"" + regDate + "\"}},VF-ALL-0-0,PERIOD";
            kv.put(ncuKey, generateCacheValue(valueKey,ncu));

            long nu = dao.countNewUser(project, regDate);
            nu = nu + ncu;
            String nuKey = "COMMON," + project + "," + regDate + "," + regDate + ",int1totalnew.*,{\"register_time\":{\"$gte\":\"" + regDate + "\",\"$lte\":\"" + regDate + "\"}},VF-ALL-0-0,PERIOD";
            kv.put(nuKey, generateCacheValue(valueKey,nu));
        }

        return kv;
    }

    public Map<String,Number[]> generateCacheValue(String key,long value){
        Map<String, Number[]> result  = new HashMap<String, Number[]>();
        result.put(key, new Number[]{0, 0, value, 1.0});
        return result;
    }

    public Map<String,Number[]> generateCacheValue(Map<String,Long> kv){
        Map<String, Number[]> result  = new HashMap<String, Number[]>();
        for(Map.Entry<String, Long> entry : kv.entrySet()) {
            result.put(entry.getKey(),new Number[]{0, 0, entry.getValue(), 1.0});
        }
        return result;
    }

    public void storeToRedis(Map<String, Long> kv, String date) {
        Map<String, Number[]> result = null;
        MapXCache xCache = null;
        XCacheOperator xCacheOperator = RedisXCacheOperator.getInstance();
        try {
            for(Map.Entry<String, Long> entry : kv.entrySet()) {
//                System.out.println(entry.getKey() + " : " + entry.getValue());
                result = new HashMap<String, Number[]>();
                result.put(date, new Number[]{0, 0, entry.getValue(), 1.0});
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
            System.out.println(key + ":" + result.keySet());
            xCache = MapXCache.buildMapXCache(key, result);
            xCacheOperator.putMapCache(xCache);
        } catch (XCacheException e) {
            e.printStackTrace();
        }
    }

    /**
     * store data to local file
     */
    public void storeToFile(Map<String, Map<String,Number[]>> result,String day) {

//        String storeFilePath = "/home/hadoop/wanghaixing/storeDatas.txt";
        String storeFilePath = BAUtil.getLocalCacheFileName(day);
        FileOutputStream out = null;
        try {
            out = new FileOutputStream(new File(storeFilePath), true);
            String content = new Gson().toJson(result);
            out.write(content.getBytes("utf-8"));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void storeFromFile(String day) throws IOException {
        String storeFilePath = BAUtil.getLocalCacheFileName(day);
        BufferedReader reader = new BufferedReader(new FileReader(storeFilePath));
        String content = reader.readLine();
        Type cacheType = new TypeToken<Map<String, Map<String,Number[]>>>(){}.getType();
        Map<String, Map<String,Number[]>> cache = new Gson().fromJson(content, cacheType);

        for(Map.Entry<String,Map<String,Number[]>> kv: cache.entrySet()){
            storeToRedisGroup(kv.getKey(),kv.getValue());
        }

    }

}

