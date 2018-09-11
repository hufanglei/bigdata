package big0903;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Arrays;

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

    @Test
    public void testDataNode()  throws Exception{
        //获取DataNode的信息（伪分布的环境）
        //指定当前的Hadoop的用户
        System.setProperty("HADOOP_USER_NAME", "root");

        //配置参数：指定NameNode地址
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.157.111:9000");

        //创建一个HDFS客户端
        //FileSystem client = FileSystem.get(conf);
        DistributedFileSystem fs = (DistributedFileSystem) FileSystem.get(conf);
        //获取数据节点的信息: Stats ---> 统计信息
        DatanodeInfo[] list = fs.getDataNodeStats();
        for(DatanodeInfo info:list){
            System.out.println(info.getHostName()+"\t"+ info.getName());
        }

        fs.close();

    }

    @Test
    public void testFileBlockLocation() throws Exception{
        //获取数据块的信息
        //指定当前的Hadoop的用户
        System.setProperty("HADOOP_USER_NAME", "root");

        //配置参数：指定NameNode地址
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.157.111:9000");

        //创建一个客户端
        FileSystem client = FileSystem.get(conf);

        //获取文件的status信息
        FileStatus fileStatus = client.getFileStatus(new Path("/folder111/a.tar.gz"));

        //获取文件的数据块信息（数组）
        BlockLocation[] locations = client.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        /*
         * 伪分布的环境，数据块的冗余度是：1
         */
        for(BlockLocation blk:locations){
            System.out.println(Arrays.toString(blk.getHosts()) + "\t" + Arrays.toString(blk.getNames()));
        }

        client.close();
    }
}
