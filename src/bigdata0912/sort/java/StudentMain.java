package bigdata0912.sort.java;

import java.util.Arrays;

public class StudentMain {


    public static void main(String[] args) {
        //创建几个学生对象
        Student s1 = new Student();
        s1.setStuID(1);
        s1.setStuName("Tom");
        s1.setAge(24);

        Student s2 = new Student();
        s2.setStuID(2);
        s2.setStuName("Mary");
        s2.setAge(26);

        Student s3 = new Student();
        s3.setStuID(3);
        s3.setStuName("Mike");
        s3.setAge(25);

        //生成一个数组
        Student[] list = {s1,s2,s3};

        //排序
        Arrays.sort(list);

        //输出
        for(Student s:list){
            System.out.println(s);
        }
    }
}
