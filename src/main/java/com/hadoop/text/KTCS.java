package com.hadoop.text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Scanner;

public class KTCS {
    public static void main(String[] args) throws IOException, InterruptedException {

        while (true){
            gn();
        }

    }

    public static void gn() throws IOException, InterruptedException {
        System.out.println("选择功能：");
        System.out.println("1.创建");
        System.out.println("2.打开");
        System.out.println("3.编辑");
        Scanner sc = new Scanner(System.in);
        int i = sc.nextInt();
        if (i == 1){
            createFile();
        }else if (i == 2){
            openFile();
        }else {
            editFile();
        }
    }




    public static void editFile() throws IOException, InterruptedException {
        FileSystem fs = getFs();
        String destPath;
        String neirong;
        String newPath;
        int n;
        Scanner sc = new Scanner(System.in);
        System.out.printf("请输入要编辑的目录及文件名:");
        destPath = sc.next();
        System.out.println("请输入要追加的内容:");
        neirong = sc.next();
        System.out.println("请选择要追加的位置:");
        System.out.println("1.开头");
        System.out.println("2.结尾");
        n = sc.nextInt();
        if (n == 1){
            FSDataOutputStream append = fs.append(new Path(destPath));
            append.write(neirong.getBytes());
            IOUtils.closeStream(append);
        }else {
            FSDataInputStream open = fs.open(new Path(destPath));
            FSDataOutputStream tempOut = fs.create(new Path("/temp.txt"),false);
            tempOut.write(neirong.getBytes());
            IOUtils.copyBytes(open,tempOut,fs.getConf());
            IOUtils.closeStream(open);
            IOUtils.closeStream(tempOut);
            FSDataOutputStream outputStream = fs.create(new Path(destPath));
            FSDataInputStream open1 = fs.open(new Path("/temp.txt"));
            IOUtils.copyBytes(open1,outputStream,fs.getConf());
            IOUtils.closeStream(open1);
            IOUtils.closeStream(outputStream);
        }
        System.out.println("请输入要保存的位置:");
        newPath = sc.next();
        fs.rename(new Path(destPath),new Path(newPath));
    }


    public static void openFile() throws IOException, InterruptedException {
        FileSystem fs = getFs();
        String destPath;
        Scanner sc = new Scanner(System.in);
        System.out.printf("请输入要打开的目录及文件名:");
        destPath = sc.next();
        FSDataInputStream open = fs.open(new Path(destPath));
        BufferedReader bf = new BufferedReader(
                new InputStreamReader(open)
        );
        String temp = null;
        while ((temp = bf.readLine())!=null){
            System.out.println(temp);
        }
        IOUtils.closeStream(bf);
        IOUtils.closeStream(open);
    }



    public static void createFile() throws IOException, InterruptedException {
        FileSystem fs = getFs();
        String destPath;
        Scanner sc = new Scanner(System.in);
        System.out.printf("请输入要创建的目录及文件名:");
        destPath = sc.next();
        FSDataOutputStream fsDataOutputStream = fs.create(new Path(destPath));
        IOUtils.closeStream(fsDataOutputStream);
    }





    private static FileSystem getFs() throws IOException, InterruptedException {
        URI uri = URI.create("hdfs://hadoop102:8020");
        Configuration configuration = new Configuration();
        String user = "machuan";
        return FileSystem.get(uri,configuration,user);
    }

}
