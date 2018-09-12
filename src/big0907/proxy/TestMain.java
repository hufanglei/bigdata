package big0907.proxy;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.InvocationHandler;


public class TestMain {
    public static void main(String[] args) {
        // 创建这个代理对象
        MyBusiness obj = new MyBusinessImpl();
        //为这个对象创建一个代理对象
        /*
	     public static Object Proxy.newProxyInstance(ClassLoader loader,  类加载器
                                                Class<?>[] interfaces, 真正对象实现的接口
                                                InvocationHandler h) 实现接口来处理客户端的调用
                               throws IllegalArgumentException
		 */
        MyBusiness proxy = (MyBusiness)Proxy.newProxyInstance(TestMain.class.getClassLoader(), obj.getClass().getInterfaces(),
                new InvocationHandler() {
            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                //处理客户端的调用
                if(method.getName().equals("method1")){
                    //执行重写
                    System.out.println("***********代理对象中的method1**************");
                    return null;
                }else{
                    //其他方法
                    return method.invoke(obj, args);
                }
            }
        });

        //通过代理对象去调用真正的对象
        proxy.method1();
        proxy.method2();

    }
}
