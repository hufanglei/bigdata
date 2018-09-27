package bigdata0912.partition;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

//代表员工
//数据：7654,MARTIN,SALESMAN,7698,1981/9/28,1250,1400,30
public class Emp implements Writable{

    private int empno;//员工号
    private String ename; //员工姓名
    private String job; //职位
    private int mgr; //经理的员工号
    private String hiredate;//入职日期
    private int sal; //月薪
    private int comm; //奖金
    private int deptno; //部门号


    @Override
    public String toString() {
        return "Emp [empno=" + empno + ", ename=" + ename + ", sal=" + sal + ", deptno=" + deptno + "]";
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        //实现反序列化，从输入流中读取对象
        this.empno = input.readInt();
        this.ename = input.readUTF();
        this.job = input.readUTF();
        this.mgr = input.readInt();
        this.hiredate = input.readUTF();
        this.sal = input.readInt();
        this.comm = input.readInt();
        this.deptno = input.readInt();
    }

    @Override
    public void write(DataOutput output) throws IOException {
        // 实现序列化，把对象输出到输出流
        output.writeInt(this.empno);
        output.writeUTF(this.ename);
        output.writeUTF(this.job);
        output.writeInt(this.mgr);
        output.writeUTF(this.hiredate);
        output.writeInt(this.sal);
        output.writeInt(this.comm);
        output.writeInt(this.deptno);
    }


    public int getEmpno() {
        return empno;
    }
    public void setEmpno(int empno) {
        this.empno = empno;
    }
    public String getEname() {
        return ename;
    }
    public void setEname(String ename) {
        this.ename = ename;
    }
    public String getJob() {
        return job;
    }
    public void setJob(String job) {
        this.job = job;
    }
    public int getMgr() {
        return mgr;
    }
    public void setMgr(int mgr) {
        this.mgr = mgr;
    }
    public String getHiredate() {
        return hiredate;
    }
    public void setHiredate(String hiredate) {
        this.hiredate = hiredate;
    }
    public int getSal() {
        return sal;
    }
    public void setSal(int sal) {
        this.sal = sal;
    }
    public int getComm() {
        return comm;
    }
    public void setComm(int comm) {
        this.comm = comm;
    }
    public int getDeptno() {
        return deptno;
    }
    public void setDeptno(int deptno) {
        this.deptno = deptno;
    }
}