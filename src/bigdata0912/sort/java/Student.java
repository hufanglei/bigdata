package bigdata0912.sort.java;

public class Student implements Comparable<Student>{

    private int stuID;
    private String stuName;
    private int age;

    @Override
    public String toString() {
        return "Student [stuID=" + stuID + ", stuName=" + stuName + ", age=" + age + "]";
    }

    @Override
    public int compareTo(Student o) {
        // 定义排序规则：按照学生的age年龄进行排序
        if(this.age >= o.getAge()){
            return 1;
        }else{
            return -1;
        }
    }

    public int getStuID() {
        return stuID;
    }

    public void setStuID(int stuID) {
        this.stuID = stuID;
    }

    public String getStuName() {
        return stuName;
    }

    public void setStuName(String stuName) {
        this.stuName = stuName;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }
}
