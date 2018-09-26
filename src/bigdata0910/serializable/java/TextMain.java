package bigdata0910.serializable.java;

import java.io.*;

public class TextMain {

    public static void main(String[] args) throws IOException {
        test2();
        return;

    }
    private static void test2() throws IOException {
        InputStream in = new FileInputStream("E:\\workplace\\ideawork\\bigdata\\src\\bigdata0910\\serializable\\java\\stu.txt");
        ObjectInputStream obj = new ObjectInputStream(in);
        System.out.println(obj.readChar());
    }

    private static void test1() throws IOException {
        Student stu = new Student();
        stu.setId(100);
        stu.setStuName("胡方雷");

        OutputStream out = new FileOutputStream("E:\\workplace\\ideawork\\bigdata\\src\\bigdata0910\\serializable\\java\\stu.txt");
        ObjectOutputStream obj = new ObjectOutputStream(out);
        obj.writeObject(stu);
        obj.close();
        out.close();
    }
}
