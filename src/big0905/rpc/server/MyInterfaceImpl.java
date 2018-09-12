package big0905.rpc.server;

import org.apache.hadoop.ipc.ProtocolSignature;

import java.io.IOException;

public class MyInterfaceImpl implements MyInterface {
    @Override
    public String sayHello(String name) {
        System.out.println("*********调用到了Server 端**********");
        return "Hello "+name;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String arg0, long arg1, int arg2) throws IOException {
        //通过版本号定义签名信息
        return new ProtocolSignature(MyInterface.versionID, null);
    }

    @Override
    public long getProtocolVersion(String arg0, long arg1) throws IOException {
        //返回版本号
        return MyInterface.versionID;
    }
}
