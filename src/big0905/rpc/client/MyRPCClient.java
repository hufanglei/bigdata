package big0905.rpc.client;

import big0905.rpc.server.MyInterface;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.net.InetSocketAddress;

public class MyRPCClient {

    public static void main(String[] args) throws Exception {
        // 使用Hadoop RPC的框架调用Server 端的程序
		/*
		RPC.getProxy(protocol,    调用的接口
				     clientVersion, 版本号
				     addr,   RPC Server的地址
				     conf)
		*/

        //得到的是Server端部署对象的代理对象
        MyInterface proxy = RPC.getProxy(MyInterface.class,
                MyInterface.versionID,
                new InetSocketAddress("localhost", 7788),
                new Configuration());

        //使用这个代理对象调用Server的程序
        String result = proxy.sayHello("Tom");
        System.out.println(result);
    }
}
