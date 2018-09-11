package big0903;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.FileOutputStream;
import java.io.OutputStream;

public class HDFSDemo {

    @Test
    public void testDownload() throws Exception{
        //数据下载
        //指定当前的Hadoop的用户
        System.setProperty("HADOOP_USER_NAME", "root");

        //配置参数：指定NameNode地址
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.157.111:9000");

        //创建一个客户端
        FileSystem client = FileSystem.get(conf);
        //构造一个输入流，从HDFS中读取数据
        FSDataInputStream input= client.open(new Path("/folder111/a.tar.gz"));
        //构造一个输出流，输出到本地的目录
        OutputStream output = new FileOutputStream("d:\\temp\\xyz.tar.gz");
        //使用工具类
        IOUtils.copyBytes(input, output, 1024);
    }
}
