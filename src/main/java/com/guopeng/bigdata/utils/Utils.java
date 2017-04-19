package com.guopeng.bigdata.utils;

import java.io.File;

/**
 * Created by guopeng on 17-4-19.
 */
public class Utils {
    public static boolean deleteDir(String dir) {
        File file = new File(dir);
        if (!file.exists())
            return false;

        if (!file.isFile()) {
            for (File f : file.listFiles()) {
                String root = f.getAbsolutePath();//得到子文件或文件夹的绝对路径
                //System.out.println(root);
                deleteDir(root);
            }
        }

        return file.delete();
    }

    public static void main(String[] args) {
        System.out.println(deleteDir("/home/guopeng/Documents/t"));
    }
}