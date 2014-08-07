package com.xingcloud.nba.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

/**
 * Created by wanghaixing on 14-8-1.
 */
public class FileManager {
    private static Log LOG = LogFactory.getLog(FileManager.class);
    /**
     * delete the file in the local disk or hdfs
     *
     * @param filePath
     *            the file tou want to delete
     * @return weather the file can be delete
     * @throws java.io.IOException
     *             the file could not delete
     */
    public static Boolean deleteFile(String filePath) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(filePath), conf);
        Path path = new Path(filePath);
        if (fs.exists(path)) {
            return fs.delete(path, true);
        }

        return false;
    }

    public static Boolean makeDir(String filePath) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(filePath), conf);
        return fs.mkdirs(new Path(filePath));
    }

   /* public static void writeToFile(String filePath) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI(filePath), conf);
        if(!fs.exists(new Path(filePath))) {

        }
    }*/

    public static boolean isFile(String file) {
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(file), conf);
            Path path = new Path(file);
            return fs.exists(path) && fs.isFile(path);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public static OutputStream createAppendStream(String file) {
        try {
            FileSystem fs = FileSystem.get(URI.create(file),
                    new Configuration());
            OutputStream out;
            if (fs.exists(new Path(file))) {
                out = fs.append(new Path(file));
            } else {
                out = fs.create(new Path(file));
            }
            return out;
        } catch (Exception e) {
            LOG.error("Exception", e);
            return null;
        }
    }

    public static void main(String[] args) throws Exception {
        String INPUT_PATH = "hdfs://65.255.35.141:19000";
        String FILE_PATH = "/d1000/f1000";
        String DIR_PATH = "/d1000";
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
        /*FSDataOutputStream out = fileSystem.append()
        OutputStream out = createAppendStream(FILE_PATH);
        String str = "abac";
        byte[] bb = str.getBytes();
        out.write(bb);*/
        fileSystem.mkdirs(new Path(DIR_PATH));
    }



}
