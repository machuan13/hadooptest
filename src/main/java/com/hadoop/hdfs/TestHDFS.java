package com.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

/*
*客户端性质开发：
* 1、获取客户端对象
* 2、调用相关方法实现功能
* 3、关闭对象
*/
public class TestHDFS {
    private FileSystem fileSystem;
    private Configuration configuration;
    private URI uri;
    private String user;

    /*
    * 文件上传
    */
    @Test
    public void testUpload() throws IOException {
        fileSystem.copyFromLocalFile(false,true,new Path("D:/Machuan/asources/1.txt"),new Path("/testhdfs"));
    }
    //@Before让注解标注的方法会在所有test标注的方法执行之前执行
    @Before
    public void init() throws IOException, InterruptedException {
        uri = URI.create("hdfs://hadoop102:8020");
        configuration = new Configuration();
        user = "machuan";

        fileSystem=FileSystem.get(uri,configuration,user);
    }
    @After
    public void close() throws IOException {
        fileSystem.close();
    }
    @Test
    public void testHDFS() throws IOException, InterruptedException {
        //1、创建文件系统对象
        URI uri= URI.create("hdfs://hadoop102:8020");
        Configuration configuration=new Configuration();
        String user="machuan";
        FileSystem fileSystem=FileSystem.get(uri,configuration,user);
        System.out.println("fs="+fileSystem);

        //2、创建目录
        boolean b=fileSystem.mkdirs(new Path("/testhdfs"));
        //3、关闭
        fileSystem.close();
    }
}
