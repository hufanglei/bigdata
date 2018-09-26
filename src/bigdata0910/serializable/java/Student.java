package bigdata0910.serializable.java;

import java.io.Serializable;

public class Student implements Serializable {
    private Integer id;
    private String stuName;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getStuName() {
        return stuName;
    }

    public void setStuName(String stuName) {
        this.stuName = stuName;
    }
}
