package demo;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

//第一级组件，作为任务的Spout组件，来采集数据
//模拟一些数据，随机产生数据
public class WordCountSpout extends BaseRichSpout {
    //定义我们要产生的数据
    private String[] datas = {"I love Beijing","I love China","Beijing is the capital of China"};

    //定义一个变量来保存输出流
    private SpoutOutputCollector collector;

    @Override
    public void nextTuple() {
        //每隔2秒采集一次
        Utils.sleep(2000);

        // 由Storm的框架调用，用于如何接受数据
        //产生一个3以内的随机数
        int random = (new Random()).nextInt(3);
        //数据
        String data = datas[random];

        //把数据发送给下一个组件
        //数据一定要遵循schema的结构
        System.out.println("采集的数据是：" + data);
        this.collector.emit(new Values(data));
    }

    @Override
    public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector collector) {
        //相当于Spout初始化方法
        //参数：SpoutOutputCollector collector 相当于是输出流
        this.collector = collector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declare) {
        // 申明Tuple的格式，是Schema
        declare.declare(new Fields("sentence"));
    }
}
