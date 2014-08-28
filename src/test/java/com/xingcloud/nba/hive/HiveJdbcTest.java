package com.xingcloud.nba.hive;

import com.xingcloud.nba.service.BAService;
import com.xingcloud.nba.utils.Constant;

import java.sql.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HiveJdbcTest {


    /*public static void main(String[] args)
            throws SQLException, ParseException {

        Connection con = HiveJdbcClient.getInstance().getConnection();
        Statement stmt = con.createStatement();

        *//*String sql = "select count(*) from  user_visit where pid='internet-2'";
        //String sql = "describe user_visit ";
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        if (res.next()) {
            System.out.println(res.getString(1));
        }

        // describe table
        System.out.println("End: " + sql);*//*

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

        Map<String,List<String>> tasks = new HashMap<String,List<String>>();
        tasks.put(Constant.INTERNET1,internet1);
        tasks.put(Constant.INTERNET2,internet2);
        String day = "2014-08-25";
        BAService service = new BAService();

        service.transProjectUID(tasks,day);

    }*/
}
