package com.xingcloud.nba.mr.model;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by wanghaixing on 14-7-30.
 */
public class JoinData implements Writable {
    private Text joinKey;
    private Text flag;
    private Text secondPart;

    public JoinData() {
        this.joinKey = new Text();
        this.flag = new Text();
        this.secondPart = new Text();
    }

    public JoinData(Text joinKey, Text flag, Text secondPart) {
        this.joinKey = joinKey;
        this.flag = flag;
        this.secondPart = secondPart;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.joinKey.write(dataOutput);
        this.flag.write(dataOutput);
        this.secondPart.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.joinKey.readFields(dataInput);
        this.flag.readFields(dataInput);
        this.secondPart.readFields(dataInput);
    }

    public Text getJoinKey() {
        return joinKey;
    }

    public void setJoinKey(Text joinKey) {
        this.joinKey = joinKey;
    }

    public Text getFlag() {
        return flag;
    }

    public void setFlag(Text flag) {
        this.flag = flag;
    }

    public Text getSecondPart() {
        return secondPart;
    }

    public void setSecondPart(Text secondPart) {
        this.secondPart = secondPart;
    }

    public String toString() {
        return "[flag="+this.flag.toString()+",joinKey="+this.joinKey.toString()+",secondPart="+this.secondPart.toString()+"]";
    }
}

