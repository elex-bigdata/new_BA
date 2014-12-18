package com.xingcloud.nba.task;


import com.xingcloud.nba.service.ScanHBaseUID3;
import com.xingcloud.nba.utils.Constant;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by wanghaixing on 14-12-17.
 */
public class WriteFileWorker implements Runnable {

    private String fileName;

    public WriteFileWorker(String fileName) {
        this.fileName = fileName;
    }

    @Override
    public void run() {

        File file = new File(fileName);
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter(file, true));
            String line = "";
            int count = 0;

            if(!file.getParentFile().exists()) {
                if(!file.getParentFile().mkdirs()) {
                    System.out.println("fail to create File！");
                }
            }
            while(true){
                try {
                    line = ScanHBaseUID3.CONTENT_QUEUE.take();
                    if(line.equals(Constant.END)) {
                        count++;
                    }
                    if(count >= 16) {
                        break;
                    }
                    if(!line.equals(Constant.END)) {
                        bw.write(line);
                    }
//                  bw.newLine();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }catch (IOException e) {
            e.printStackTrace();
        }

    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }


}
