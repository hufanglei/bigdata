
//运行 java -Dkey1=value1 TestD
public class TestD {

    public static void main(String[] args){
        method1();
    }


    public static void method1(){
        String value = System.getProperty("key1");
        System.out.println("value is:" + value);
    }
}
