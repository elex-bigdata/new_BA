package com.xingcloud.nba.task;

import com.xingcloud.nba.utils.Constant;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Author: liqiang
 * Date: 14-8-28
 * Time: 下午3:04
 */
public class InternetDAOTest {

    /*public static InternetDAO dao = new InternetDAO();
    public static String day = "2014-08-27";
    public static Map<String,List<String>> tasks = new HashMap<String,List<String>>();

    @BeforeClass
    public static void init(){
        List<String> internet1 = new ArrayList<String>();
        internet1.add("webssearches");
        internet1.add("key-find");
        internet1.add("awesomehp");
        internet1.add("sweet-page");
        internet1.add("v9");
        internet1.add("do-search");
        internet1.add("aartemis");
        internet1.add("omiga-plus");
        internet1.add("qone8");
        internet1.add("dosearches");
        internet1.add("delta-homes");
        internet1.add("22apple");
        internet1.add("22find");
        internet1.add("qvo6");
        internet1.add("portaldosites");
        internet1.add("nationzoom");
        internet1.add("usv9");
        internet1.add("istart123");
        internet1.add("vi-view:");
        internet1.add("istartsurf");

        List<String> internet2 = new ArrayList<String>();
        internet2.add("sof-wpm");
        internet2.add("sof-yacnvd");
        internet2.add("sof-newgdp");
        internet2.add("sof-dsk");
        internet2.add("sof-dp");
        internet2.add("sof-gdp");
        internet2.add("sof-zip");
        internet2.add("sof-isafe");
        internet2.add("sof-hpprotect");
        internet2.add("sof-windowspm");
        internet2.add("sof-ient");

        tasks.put(Constant.INTERNET1,internet1);
        tasks.put(Constant.INTERNET2,internet2);
        List<String> aids = new ArrayList<String>();
        aids.addAll(internet1);
        aids.addAll(internet2);
        tasks.put(Constant.INTERNET,aids);
    }

    @Test
    public void testNewUser() throws Exception {
        long nu = dao.countNewUser(day, Constant.INTERNET1);
        System.out.println(Constant.INTERNET1 + " new: " + nu);
        nu = dao.countNewUser(day, Constant.INTERNET2);
        System.out.println(Constant.INTERNET2 + " new: " + nu);
        nu = dao.countNewUser(day, Constant.INTERNET);
        System.out.println(Constant.INTERNET + " new: " + nu);
    }

    @Test
    public void testActiveUser() throws Exception {
        day = "2014-08-26";
        String[] days= new String[]{day};
        long nu = dao.countActiveUser(days, Constant.INTERNET1);
        System.out.println(Constant.INTERNET1 + " active: " + nu);
        nu = dao.countActiveUser(days, Constant.INTERNET2);
        System.out.println(Constant.INTERNET2 + " active: " + nu);
        nu = dao.countActiveUser(days, Constant.INTERNET);
        System.out.println(Constant.INTERNET + " active: " + nu);
    }

    @Test
    public void testGroupByGeoip() throws Exception {
        Map<String,Long> result = dao.countNewUserByGeoip(day,Constant.INTERNET1);
        for(Map.Entry<String,Long> kv : result.entrySet() ){
            System.out.println(kv.getKey() + " : " + kv.getValue());
        }

*//*        Map<String,Long> result = dao.countRetentionUserByGeoip("2014-08-26",new String[]{"2014-08-27"},Constant.INTERNET1);
        for(Map.Entry<String,Long> kv : result.entrySet() ){
            System.out.println(kv.getKey() + " : " + kv.getValue());
        }*//*
    }

    @Test
    public void testRetention() throws Exception {
        long retention = dao.countRetentionUser("2014-08-26",new String[]{"2014-08-27"},Constant.INTERNET1);
        System.out.println(Constant.INTERNET1 + " retention : " +  retention);
        retention = dao.countRetentionUser("2014-08-26",new String[]{"2014-08-27"},Constant.INTERNET1);
        System.out.println(Constant.INTERNET1 + " retention : " +  retention);
        retention = dao.countRetentionUser("2014-08-26",new String[]{"2014-08-27"},Constant.INTERNET);
        System.out.println(Constant.INTERNET1 + " retention : " +  retention);

    }*/

}
