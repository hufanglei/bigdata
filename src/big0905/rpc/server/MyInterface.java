package big0905.rpc.server;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface MyInterface extends VersionedProtocol {
    //定义一个版本号
    //使用版本号来进行签名
    public static long versionID = 1;

    //定义我们业务方法
    public String sayHello(String name);
}
