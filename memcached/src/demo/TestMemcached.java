package demo;

import net.spy.memcached.internal.OperationFuture;
import org.junit.Test;
import net.spy.memcached.MemcachedClient;


import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

public class TestMemcached {
    @Test
    public void testInsert() throws Exception{
        //插入一个字符串
        //得到客户端
        MemcachedClient client = new MemcachedClient(new InetSocketAddress("192.168.157.111", 11211));
        Future<Boolean> f = client.set("key1", 0, "Hello World");
        if(f.get().booleanValue()){
            //表示成功
            client.shutdown();
        }
    }

    @Test
    public void testInsertObjext() throws Exception{
        //插入一个对象
        //创建对象
        Student s = new Student();
        //得到客户端
        MemcachedClient client = new MemcachedClient(new InetSocketAddress("192.168.157.111", 11211));
        Future<Boolean> f = client.set("s001", 0, s);
        if(f.get().booleanValue()){
            //表示成功
            client.shutdown();
        }
    }


    @Test
    public void testGet() throws Exception{
        //查询
        //得到客户端
        MemcachedClient client = new MemcachedClient(new InetSocketAddress("192.168.157.111", 11211));
        Object value = client.get("key1");
        System.out.println(value);
        client.shutdown();
    }

    //测试MemCached的客户端路由
    @Test
    public void testInsertList() throws Exception{
        //查询
        //创建一个List加入所有的MemCached实例
        List<InetSocketAddress> list = new ArrayList<>();
        list.add(new InetSocketAddress("192.168.157.111", 11211));
        list.add(new InetSocketAddress("192.168.157.111", 11212));
        list.add(new InetSocketAddress("192.168.157.111", 11213));
        MemcachedClient client = new MemcachedClient(list);
        //插入20条数据
        for(int i=0;i<20;i++){
            System.out.println("插入的数据是： key="+("key"+i)+"     value是"+("value"+i));
            client.set("key"+i, 0, "value"+i);

            //睡1秒
            Thread.sleep(1000);
        }

        //关闭客户端
        client.shutdown();
    }
}
//定义一个类
class Student implements Serializable {

}