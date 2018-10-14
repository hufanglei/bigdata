package demo;

import java.io.IOException;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;
public class IsSalaryTooHigh extends FilterFunc {
    @Override
    public Boolean exec(Tuple tuple) throws IOException {
        /*
         * 过滤的逻辑：查询工资大于2000的员工
         * 调用：result1 = filter emp by demo.pig.IsSalaryTooHigh(sal);
         */
        //取出薪水
        int salary = (int) tuple.get(0);
        return salary>2000?true:false;
    }
}
