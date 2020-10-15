package com.hadoop.text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.Scanner;

public class Hdfstext {
    private  static FileSystem fileSystem;
    private static Configuration configuration;
    private static URI uri;
    private static String user;

    public static void init() throws IOException, InterruptedException {
        uri = URI.create("hdfs://hadoop102:8020");
        configuration = new Configuration();
        user = "machuan";

        fileSystem=FileSystem.get(uri,configuration,user);
    }

    public static void close() throws IOException {
        fileSystem.close();
    }
    public static void jiemian(){
        System.out.println("hdfs文本编辑器");
        System.out.println("1、新建");
        System.out.println("2、打开");
        System.out.println("3、编辑");
        System.out.println("4、保存");
    }

    public static void main(String args[]) throws IOException, InterruptedException {
        init();
        jiemian();
        Scanner input =new Scanner(System.in);
        Integer choose=input.nextInt();
        switch (choose){
            case 1:write();break;
            case 2:read();break;
            case 3:bianji();break;
        }
        close();
    }
    //1,新建
    public static void write() throws IOException {
        System.out.println("请输入创建文本名称");
        Scanner sc=new Scanner(System.in);
        String name=sc.next();
        String path ="D:\\Machuan\\asources\\"+name;
        String path1="D:/Machuan/asources/"+name;
        FileWriter fw =new FileWriter(path,true);
        System.out.println(path1);
        fileSystem.copyFromLocalFile(false,true,new Path(path1),new Path("/hdfstext"));
    }
    //2、打开
    public static void read(){
        FSDataInputStream fsDataInputStream =null;
        Scanner sc2 =new Scanner(System.in);
        String filepath =sc2.next();
        try {
            Path path =new Path(filepath);
            fsDataInputStream=fileSystem.open(path);
            IOUtils.copyBytes(fsDataInputStream, System.out, 4096, false);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if (fsDataInputStream!=null){
                IOUtils.closeStream(fsDataInputStream);
            }
        }
    }
    //3、编辑
    public static void bianji() throws IOException {
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
            FSDataOutputStream append = fileSystem.append(new Path(destPath));
            append.write(neirong.getBytes());
            IOUtils.closeStream(append);
        }else {
            FSDataInputStream open = fileSystem.open(new Path(destPath));
            FSDataOutputStream tempOut = fileSystem.create(new Path("/temp.txt"),false);
            tempOut.write(neirong.getBytes());
            IOUtils.copyBytes(open,tempOut,fileSystem.getConf());
            IOUtils.closeStream(open);
            IOUtils.closeStream(tempOut);
            FSDataOutputStream outputStream = fileSystem.create(new Path(destPath));
            FSDataInputStream open1 = fileSystem.open(new Path("/temp.txt"));
            IOUtils.copyBytes(open1,outputStream,fileSystem.getConf());
            IOUtils.closeStream(open1);
            IOUtils.closeStream(outputStream);
        }
        System.out.println("请输入要保存的位置:");
        newPath = sc.next();
        fileSystem.rename(new Path(destPath),new Path(newPath));
    }
}
