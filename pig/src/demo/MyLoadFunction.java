package demo;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class MyLoadFunction extends LoadFunc {
    //代表输入流
    private RecordReader reader;

    @Override
    public InputFormat getInputFormat() throws IOException {
        // 指定输入数据的类型：字符串
        return new TextInputFormat();
    }

    @Override
    public Tuple getNext() throws IOException {
        //从输入流中，读入一行数据后，如何解析
        //1、每个单词作为一个Tuple
        //2、这些tuple再放入一个bag中
        //3、再把这个bag放入返回的tuple中
        //数据: I love Beijing
        Tuple result = null;
        try{
            //判断是否有数据
            if(!reader.nextKeyValue()){
                //没有数据
                return result; //就是空值
            }

            //有数据读入
            String data = reader.getCurrentValue().toString();
            //分词
            String[] words = data.split(" ");

            //生成返回结果的Tuple
            result = TupleFactory.getInstance().newTuple();

            //每个单词放入一个Tuple中，再把这些Tuple放入一个表中
            //生成一张表
            DataBag bag = BagFactory.getInstance().newDefaultBag();
            for(String w:words){
                //取出每个单词
                Tuple aTuple = TupleFactory.getInstance().newTuple();
                aTuple.append(w);

                //加入表
                bag.add(aTuple);
            }

            //把表放入Tuple中
            result.append(bag);
        }catch(Exception ex){
            ex.printStackTrace();
        }
        return result;
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit arg1) throws IOException {
        // 使用该方法，初始化输入流 : RecordReader
        this.reader= reader;
    }

    @Override
    public void setLocation(String path, Job job) throws IOException {
        //指定HDFS的目录
        FileInputFormat.setInputPaths(job, new Path(path));
    }
}
