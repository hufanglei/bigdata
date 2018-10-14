package demo;
import org.apache.hadoop.hive.ql.exec.UDF;

/*
 * 根据员工的薪水，判断薪水的级别
 (*) sal < 1000     ----> Grade A
 (*) 1000<=sal <3000 ---> Grade B
 (*) sal >=3000      ---> Grade C
 */
public class MyConcatString extends UDF {
    //必须重写一个方法，名字必须叫: evaluate
    public String evaluate(String a,String b){
        return a+"**************"+b;
    }
}