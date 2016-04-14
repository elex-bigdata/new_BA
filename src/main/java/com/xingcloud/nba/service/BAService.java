package com.xingcloud.nba.service;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.xingcloud.maincache.InterruptQueryException;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.log4j.Logger;

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

    //TODO: 改造成 key SQL作为参数的形式，以适应并发请求

    public InternetDAO dao = new InternetDAO();
    private static final Logger LOGGER = Logger.getLogger(BAService.class);


    public void alterTable(Map<String,List<String>> projects, String day) throws Exception {
        dao.alterTable(projects.get(Constant.INTERNET), day);
        dao.alterTable(projects.get(Constant.RAFONAV), day);
        dao.alterTable(projects.get(Constant.RAFOCLIENT), day);
    }

    public void initPartition(Map<String,List<String>> projects) throws SQLException {
        String[] attrs = new String[]{"register_time", "nation"};
        dao.initPartition(projects.get(Constant.INTERNET), attrs);
        dao.initPartition(projects.get(Constant.RAFONAV), attrs);
        dao.initPartition(projects.get(Constant.RAFOCLIENT), attrs);
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
        // add two project
        String transRafoNavVisitSQL = BASQLGenerator.getTransVistUIDSql(Constant.RAFONAV, tasks.get(Constant.RAFONAV), day);
        String transRafoClientVisitSQL = BASQLGenerator.getTransVistUIDSql(Constant.RAFOCLIENT, tasks.get(Constant.RAFOCLIENT), day);

        String transIntVisitSQL = BASQLGenerator.getCombineVisitUIDSql(Constant.INTERNET, day, new String[]{Constant.INTERNET1, Constant.INTERNET2});
        String[] transVisit = new String[]{transInt1VisitSQL,transInt2VisitSQL,transIntVisitSQL,transRafoNavVisitSQL,transRafoClientVisitSQL};


        //将注册时间转成原始UID去重放到user_register_time各个分区
        String transInt1RegSQL = BASQLGenerator.getTransRegisterTimeUIDSql(Constant.INTERNET1, tasks.get(Constant.INTERNET1));
        String transInt2RegSQL = BASQLGenerator.getTransRegisterTimeUIDSql(Constant.INTERNET2, tasks.get(Constant.INTERNET2));
        // add two project
        String transRafoNavRegSQL = BASQLGenerator.getTransRegisterTimeUIDSql(Constant.RAFONAV, tasks.get(Constant.RAFONAV));
        String transRafoClientRegSQL = BASQLGenerator.getTransRegisterTimeUIDSql(Constant.RAFOCLIENT, tasks.get(Constant.RAFOCLIENT));

        String transIntRegSQL = BASQLGenerator.getCombineRegisterTimeUIDSql(Constant.INTERNET, new String[]{Constant.INTERNET1, Constant.INTERNET2});
        String[] transReg = new String[]{transInt1RegSQL,transInt2RegSQL,transIntRegSQL,transRafoNavRegSQL,transRafoClientRegSQL};

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
        String valueKey = day + " 00:00";
        for(String project : projects){

            //日
            String[] days = new String[]{day};
            long dau = dao.countActiveUser(project, days);
            String dauKey = "COMMON," + project + "," + day + "," + day + ",visit.*,TOTAL_USER,VF-ALL-0-0,PERIOD";

            if(project.equals(Constant.INTERNET1)) {
                long pv = getPVValue(dauKey);
                Map<String, Number[]> result  = generateCacheValue(valueKey, pv, dau);
                kv.put(dauKey,result);
                //internet中类目1
                String categoryKey = "COMMON,internet," + day + "," + day + ",visit.*,{\"category\":\"1\"},VF-ALL-0-0,PERIOD";
                kv.put(categoryKey,result);
            } else if(project.equals(Constant.INTERNET2)) {
                Map<String, Number[]> result  = generateCacheValue(valueKey,dau);
                kv.put(dauKey,result);
                //internet中类目2
                String categoryKey = "COMMON,internet," + day + "," + day + ",visit.*,{\"category\":\"2\"},VF-ALL-0-0,PERIOD";
                kv.put(categoryKey,result);
            } else {
                kv.put(dauKey,generateCacheValue(valueKey,dau));
            }

            //周
//            String[] days = new String[]{day};
            String beginDate = DateManager.getDaysBefore(date,7,0);
            days = new String[]{beginDate,day};
            long wau = dao.countActiveUser(project, days);
            String wauKey = "COMMON," + project + "," + beginDate + "," + day + ",visit.*,TOTAL_USER,VF-ALL-0-0,PERIOD";
            kv.put(wauKey,generateCacheValue(valueKey,wau));

            //月
            beginDate = DateManager.getDaysBefore(date,30,0);
            days = new String[]{beginDate,day};
            long mau = dao.countActiveUser(project, days);
            String mauKey = "COMMON," + project + "," + beginDate + "," + day + ",visit.*,TOTAL_USER,VF-ALL-0-0,PERIOD";
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


    //GROUP,internet-1,2015-01-08,2015-01-08,visit.*,TOTAL_USER,VF-ALL-0-0,USER_PROPERTIES,geoip
    //COMMON,internet-1,2015-01-08,2015-01-08,visit.*,{"geoip":"us"},VF-ALL-0-0,PERIOD
    public Map<String, Map<String,Number[]>> calActiveUserByAttr(Set<String> projects, String attr, String day) throws Exception {
        String[] days = new String[]{day};

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
            result = dao.countActiveUserByAttr(project, attr, days);
            for(Map.Entry<String, Long> entry : result.entrySet()) {
                key = "COMMON," + project + "," + visitDate + "," + visitDate + ",visit.*,{\""+attr+"\":\"" + entry.getKey() + "\"},VF-ALL-0-0,PERIOD";
                //common
                commonKV.put(key,generateCacheValue(valueKey,entry.getValue()));
            }
            //group
            key = "GROUP," + project + "," + visitDate + "," + visitDate + ",visit.*,TOTAL_USER,VF-ALL-0-0,USER_PROPERTIES,"+attr;
            commonKV.put(key,generateCacheValue(result));

        }

        return commonKV;
    }

    //COMMON,age,2014-08-25,2014-08-25,visit.*,{"geoip":"ua","register_time":{"$gte":"2014-08-25","$lte":"2014-08-25"}},VF-ALL-0-0,PERIOD

    //GROUP,age,2014-08-25,2014-08-25,visit.*,{"register_time":{"$gte":"2014-08-25","$lte":"2014-08-25"}},VF-ALL-0-0,USER_PROPERTIES,geoip
    public Map<String, Map<String,Number[]>> calNewUserByAttr(Set<String> projects, String attr, String day) throws Exception {

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

        return commonKV;
    }

    //COMMON,age,2014-08-26,2014-08-26,visit.*,{"geoip":"ua","register_time":{"$gte":"2014-08-25","$lte":"2014-08-25"}},VF-ALL-0-0,PERIOD

    //GROUP,age,2014-08-26,2014-08-26,visit.*,{"register_time":{"$gte":"2014-08-25","$lte":"2014-08-25"}},VF-ALL-0-0,USER_PROPERTIES,geoip
    public Map<String, Map<String,Number[]>> calRetentionUserByAttr(Set<String> projects, String attr, String day) throws Exception {
        Date date = DateManager.dayfmt.parse(day);
        String visitDate = DateManager.getDaysBefore(date, 0, 0);

        Map<String,Map<String,Number[]>> kv = new HashMap<String, Map<String,Number[]>>();
        Map<String,Long> result = null;
        String key = "";

        String day2 = DateManager.getDaysBefore(date, 1, 0);
        String day3 = DateManager.getDaysBefore(date, 2, 0);
        String day4 = DateManager.getDaysBefore(date, 3, 0);
        String day5 = DateManager.getDaysBefore(date, 4, 0);
        String day6 = DateManager.getDaysBefore(date, 5, 0);
        String day7 = DateManager.getDaysBefore(date, 6, 0);

        String[] singleDayRegDates = new String[]{day2,day7};

        for(String project : projects) {
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
                kv.put(key, generateCacheValue(day6 + " 00:00",entry.getValue()));
            }
            //group
            key = "GROUP," + project + "," + day6 + "," + visitDate + ",visit.*,{\"register_time\":{\"$gte\":\"" + day7 + "\",\"$lte\":\"" + day7 + "\"}},VF-ALL-0-0,USER_PROPERTIES,"+attr;
            kv.put(key,generateCacheValue(result));

            result = dao.countRetentionUserByAttr(project, attr, day6, new String[]{day5,visitDate});
            key = "GROUP," + project + "," + day5 + "," + visitDate + ",visit.*,{\"register_time\":{\"$gte\":\"" + day6 + "\",\"$lte\":\"" + day6 + "\"}},VF-ALL-0-0,USER_PROPERTIES,"+attr;
            kv.put(key,generateCacheValue(result));
            result = dao.countRetentionUserByAttr(project, attr, day5, new String[]{day4,visitDate});
            key = "GROUP," + project + "," + day4 + "," + visitDate + ",visit.*,{\"register_time\":{\"$gte\":\"" + day5 + "\",\"$lte\":\"" + day5 + "\"}},VF-ALL-0-0,USER_PROPERTIES,"+attr;
            kv.put(key,generateCacheValue(result));
            result = dao.countRetentionUserByAttr(project, attr, day4, new String[]{day3,visitDate});
            key = "GROUP," + project + "," + day3 + "," + visitDate + ",visit.*,{\"register_time\":{\"$gte\":\"" + day4 + "\",\"$lte\":\"" + day4 + "\"}},VF-ALL-0-0,USER_PROPERTIES,"+attr;
            kv.put(key,generateCacheValue(result));
            result = dao.countRetentionUserByAttr(project, attr, day3, new String[]{day2,visitDate});
            key = "GROUP," + project + "," + day3 + "," + visitDate + ",visit.*,{\"register_time\":{\"$gte\":\"" + day3 + "\",\"$lte\":\"" + day3 + "\"}},VF-ALL-0-0,USER_PROPERTIES,"+attr;
            kv.put(key,generateCacheValue(result));
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

    public Map<String,Number[]> generateCacheValue(String key, long pv, long value){
        Map<String, Number[]> result  = new HashMap<String, Number[]>();
        result.put(key, new Number[]{pv, 0, value, 1.0});
        return result;
    }

    public Map<String,Number[]> generateCacheValue(Map<String,Long> kv){
        Map<String, Number[]> result  = new HashMap<String, Number[]>();
        for(Map.Entry<String, Long> entry : kv.entrySet()) {
            result.put(entry.getKey(),new Number[]{0, 0, entry.getValue(), 1.0});
        }
        return result;
    }

    //清理大于30天的数据，中间结果等
    public void cleanup(){
        //drop 分区
    }

    public void storeToRedis(Map<String, Map<String,Number[]>> cache) {
        MapXCache xCache = null;
        XCacheOperator xCacheOperator = RedisXCacheOperator.getInstance();
        try {
            for(Map.Entry<String,Map<String,Number[]>> kv: cache.entrySet()){
//                System.out.println(kv.getKey() + ":" + kv.getValue().keySet());
                xCache = MapXCache.buildMapXCache(kv.getKey(), kv.getValue());
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
    public void storeToFile(Map<String, Map<String,Number[]>> result,String day, boolean toHdfs) {
        String storeFilePath = BAUtil.getLocalCacheFileName(day);
        FileOutputStream out = null;
        try {
            out = new FileOutputStream(new File(storeFilePath), true);
            String content = new Gson().toJson(result);
            out.write(content.getBytes("utf-8"));

            if(toHdfs){
                FileSystem fs = FileSystem.get(new Configuration());
                fs.copyFromLocalFile(new Path(storeFilePath), new Path(Constant.HDFS_CATCH_PATH));
            }
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

        storeToRedis(cache);

    }

    public long getPVValue(String cacheKey) throws XCacheException, InterruptQueryException {
        long pvValue = 0;
        if (Strings.isNullOrEmpty(cacheKey)) {
            System.out.println("cacheKey is null");
        }

        // 获取cache接口
        XCacheOperator cacheOperator = RedisXCacheOperator.getInstance();
        MapXCache cache;
        try {
            cache = cacheOperator.getMapCache(cacheKey);
        } catch (XCacheException e) {
            throw e;
        }

        if (cache == null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("[CACHE] - MISSED - " + cacheKey);
            }
            return 0;
        }

        // 查询Cache
        Map<String, Number[]> numberMap = cache.toNumberArrayMap();
        if (numberMap == null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("[CACHE] - HIT, NULL-CONTENT - " + cacheKey);
            }
            return 0;
        }

        for(Map.Entry<String, Number[]> entry : numberMap.entrySet()) {
            Number[] nums = entry.getValue();
            pvValue = nums[0].longValue();
        }

        return pvValue;
    }

}

