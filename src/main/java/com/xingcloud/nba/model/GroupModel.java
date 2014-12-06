package com.xingcloud.nba.model;

import com.google.gson.internal.Pair;

import java.util.Map;

/**
 * Created by wanghaixing on 14-12-4.
 */
public class GroupModel {
    private Pair<String,CacheModel> nation;
    private Map<String,CacheModel> ev3;
    private Map<String,CacheModel> ev4;
    private Map<String,CacheModel> ev5;

    public Pair<String, CacheModel> getNation() {
        return nation;
    }

    public void setNation(Pair<String, CacheModel> nation) {
        this.nation = nation;
    }

    public Map<String, CacheModel> getEv3() {
        return ev3;
    }

    public void setEv3(Map<String, CacheModel> ev3) {
        this.ev3 = ev3;
    }

    public Map<String, CacheModel> getEv4() {
        return ev4;
    }

    public void setEv4(Map<String, CacheModel> ev4) {
        this.ev4 = ev4;
    }

    public Map<String, CacheModel> getEv5() {
        return ev5;
    }

    public void setEv5(Map<String, CacheModel> ev5) {
        this.ev5 = ev5;
    }
}
