package big0903;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;


/*
 * 错误：
 * Permission denied: user=lenovo, access=WRITE, inode="/folder111":root:supergroup:d rwx  r-x  r-x
 *
 * 针对其他用户，没有w的权限
 *
 * 四种方式可以改变HDFS的权限：
 *
 * 第一种方式：设置环境变量  HADOOP_USER_NAME = root
 * 第二种方式：通过使用Java的 -D参数
 * 第三种方式：dfs.permissions ----> false
 * 第四种方式：命令 -chmod 改变HDFS目录的权限
 */
public class TestMkDir {

	@Test
	public void testMkDir1() throws Exception{
		//指定当前的Hadoop的用户
		System.setProperty("HADOOP_USER_NAME", "root");

		//配置参数：指定NameNode地址
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.157.111:9000");

		//创建一个客户端
		FileSystem client = FileSystem.get(conf);

		//创建目录
		client.mkdirs(new Path("/folder111"));

		//关闭客户端
		client.close();
	}

	@Test
	public void testMkDir2() throws Exception{
		//配置参数：指定NameNode地址
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.157.111:9000");

		//创建一个客户端
		FileSystem client = FileSystem.get(conf);

		//创建目录
		client.mkdirs(new Path("/folder222"));

		//关闭客户端
		client.close();
	}

	@Test
	public void testMkDir4() throws Exception{
		//配置参数：指定NameNode地址
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.157.111:9000");

		//创建一个客户端
		FileSystem client = FileSystem.get(conf);

		//创建目录
		client.mkdirs(new Path("/folder222/folder444"));

		//关闭客户端
		client.close();
	}
}















