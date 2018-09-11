package big0903;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;

public class TestUpload {


    @Test
    public void test1() throws Exception{
        //指定当前的Hadoop的用户
        System.setProperty("HADOOP_USER_NAME", "root");

        //配置参数：指定NameNode地址
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.157.111:9000");

        //创建一个客户端
        FileSystem client = FileSystem.get(conf);

        //构造一个输入流，从本地读入数据
        InputStream input = new FileInputStream("D:\\BaiduNetdiskDownload\\tree-1.7.0.tgz");
        //构造一个输出流 指向HDFS
        OutputStream output = client.create(new Path("/folder111/a.tar.gz"));
        //构造一个缓冲区
        byte[] buffer = new byte[1024];
        //长度
        int len = 0;
        //读入数据
        while((len=input.read(buffer)) > 0){
            //写到输出流中
            output.write(buffer, 0, len);
        }

        output.flush();

        //关闭流
        input.close();
        output.close();
    }

    @Test
    public void test2() throws Exception{
        //指定当前的Hadoop的用户
        System.setProperty("HADOOP_USER_NAME", "root");

        //配置参数：指定NameNode地址
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.157.111:9000");

        //创建一个客户端
        FileSystem client = FileSystem.get(conf);

        //构造一个输入流，从本地读入数据
        InputStream input = new FileInputStream("d:\\temp\\hadoop-2.7.3.tar.gz");

        //构造一个输出流 指向HDFS
        OutputStream output = client.create(new Path("/folder111/b.tar.gz"));

        //使用HDFS的一个工具类简化代码
        IOUtils.copyBytes(input, output, 1024);
    }

}
