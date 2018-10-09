package demo.base;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

/*
 * 常见的过滤器
 * 1、列值过滤器：  select * from emp where sal=3000;
 * 2、列名前缀过滤器：查询员工的姓名   select ename from emp;
 * 3、多个列名前缀过滤器：查询员工的姓名、薪水   select ename,sal from emp;
 * 4、行键过滤器：通过Rowkey查询，类似通过Get查询数据
 * 5、组合几个过滤器查询数据：where 条件1 and（or） 条件2
 */
public class TestHBaseFilter {

    @Test
    public void testSingleColumnValueFilter() throws Exception{
        //指定的配置信息: ZooKeeper
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "192.168.157.111");

        //客户端
        HTable table = new HTable(conf, "emp");

        //定义一个列值过滤器
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("empinfo"),     //列族
                Bytes.toBytes("sal"),  //列名
                CompareOp.EQUAL,  //比较运算符
                Bytes.toBytes("3000"));     //值

        //把过滤器加入扫描器
        Scan scan = new Scan();
        scan.setFilter(filter);

        //查询数据
        ResultScanner rs = table.getScanner(scan);
        for(Result r:rs){
            String name = Bytes.toString(r.getValue(Bytes.toBytes("empinfo"), Bytes.toBytes("ename")));
            System.out.println(name);
        }

        table.close();
    }


    @Test
    public void testColumnPrefixFilter() throws Exception{
        //指定的配置信息: ZooKeeper
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "192.168.157.111");

        //客户端
        HTable table = new HTable(conf, "emp");

        //定义列名前缀过滤器
        ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes("ename"));

        //把过滤器加入扫描器
        Scan scan = new Scan();
        scan.setFilter(filter);

        //查询数据：结果中只有员工的姓名
        ResultScanner rs = table.getScanner(scan);
        for(Result r:rs){
            //获取姓名、薪水
            String name = Bytes.toString(r.getValue(Bytes.toBytes("empinfo"), Bytes.toBytes("ename")));
            String sal = Bytes.toString(r.getValue(Bytes.toBytes("empinfo"), Bytes.toBytes("sal")));

            System.out.println(name+"\t"+sal);
        }

        table.close();
    }

    @Test
    public void testMultipleColumnPrefixFilter() throws Exception{
        //指定的配置信息: ZooKeeper
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "192.168.157.111");

        //客户端
        HTable table = new HTable(conf, "emp");

        //定义一个二维的字节数据，代表多个列名
        byte[][] names = {Bytes.toBytes("ename"),Bytes.toBytes("sal")};

        //定义多个列名前缀过滤器，查询：姓名和薪水
        MultipleColumnPrefixFilter filter = new MultipleColumnPrefixFilter(names);

        //把过滤器加入扫描器
        Scan scan = new Scan();
        scan.setFilter(filter);

        //查询数据：结果中只有员工的姓名
        ResultScanner rs = table.getScanner(scan);
        for(Result r:rs){
            //获取姓名、薪水
            String name = Bytes.toString(r.getValue(Bytes.toBytes("empinfo"), Bytes.toBytes("ename")));
            String sal = Bytes.toString(r.getValue(Bytes.toBytes("empinfo"), Bytes.toBytes("sal")));

            System.out.println(name+"\t"+sal);
        }

        table.close();
    }

    @Test
    public void testRowFilter() throws Exception{
        //指定的配置信息: ZooKeeper
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "192.168.157.111");

        //客户端
        HTable table = new HTable(conf, "emp");

        //定义一个RowFilter 行键过滤器: 查询rowkey=7839的员工
        RowFilter filter = new RowFilter(CompareOp.EQUAL,   //比较运算符
                new RegexStringComparator("7839")); //行键的值：使用的一个正则表达式
        //把过滤器加入扫描器
        Scan scan = new Scan();
        scan.setFilter(filter);

        //查询数据：结果中只有员工的姓名
        ResultScanner rs = table.getScanner(scan);
        for(Result r:rs){
            //获取姓名、薪水
            String name = Bytes.toString(r.getValue(Bytes.toBytes("empinfo"), Bytes.toBytes("ename")));
            String sal = Bytes.toString(r.getValue(Bytes.toBytes("empinfo"), Bytes.toBytes("sal")));

            System.out.println(name+"\t"+sal);
        }

        table.close();
    }

    @Test
    public void testFilter() throws Exception{
        //组合两个过滤器
        //举例：查询工资等于3000的员工姓名
        /*
         * 第一个过滤器：列值过滤器
         * 第二个过滤器：列名前缀过滤器
         */

        //指定的配置信息: ZooKeeper
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "192.168.157.111");

        //客户端
        HTable table = new HTable(conf, "emp");

        //定义一个列值过滤器
        SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes("empinfo"),     //列族
                Bytes.toBytes("sal"),  //列名
                CompareOp.EQUAL,  //比较运算符
                Bytes.toBytes("3000"));     //值
        //定义列名前缀过滤器
        ColumnPrefixFilter filter2 = new ColumnPrefixFilter(Bytes.toBytes("ename"));

        //创建一个FilterList
        FilterList list = new FilterList(Operator.MUST_PASS_ALL);  //相当于 and
        //FilterList list = new FilterList(Operator.MUST_PASS_ONE); // 相当于or
        list.addFilter(filter1);
        list.addFilter(filter2);

        //把过滤器加入扫描器
        Scan scan = new Scan();
        scan.setFilter(list);

        //查询数据：结果中只有员工的姓名
        ResultScanner rs = table.getScanner(scan);
        for(Result r:rs){
            //获取姓名、薪水
            String name = Bytes.toString(r.getValue(Bytes.toBytes("empinfo"), Bytes.toBytes("ename")));
            String sal = Bytes.toString(r.getValue(Bytes.toBytes("empinfo"), Bytes.toBytes("sal")));

            System.out.println(name+"\t"+sal);
        }

        table.close();
    }
}




