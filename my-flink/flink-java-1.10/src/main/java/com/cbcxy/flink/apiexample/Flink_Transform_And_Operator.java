package com.cbcxy.flink.apiexample;


import com.cbcxy.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;


public class Flink_Transform_And_Operator {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1.读取数据
        DataStreamSource<String> inputDS = env.readTextFile("input/sensor-data.log");

        DataStreamSource<List<Integer>> collectionDS = env.fromCollection(
                Arrays.asList(
                        Arrays.asList(1, 2, 3, 4),
                        Arrays.asList(5, 6, 7, 8)
                )
        );

        DataStreamSource<Integer> numDS0 = env.fromCollection(
                Arrays.asList(1, 2, 3, 4)
        );
        DataStreamSource<Integer> numDS1 = env.fromCollection(Arrays.asList(11, 12, 13, 14));
        DataStreamSource<Integer> numDS2 = env.fromCollection(Arrays.asList(21, 22, 23, 24));

        // TODO 2.1 Transform: Map , MapFunction 转换成实体对象
        SingleOutputStreamOperator<WaterSensor> sensorDS = inputDS.map(new MyMapFunction());

        // TODO 2.2 Transform: Map , RichMapFunction 转换成实体对象
        SingleOutputStreamOperator<WaterSensor> richSensorDS = inputDS.map(new MyRichMapFunction());

        // TODO 2.3 Transform: flatMap , FlatMapFunction 转换成实体对象
        collectionDS.flatMap((FlatMapFunction<List<Integer>, Integer>) (value, out) -> {
                    for (Integer number : value) {
                        out.collect(number + 10);
                    }
                });

        // TODO 2.4 Transform: filter , FilterFunction  true保留 false丢弃
        numDS0.filter(new MyFilterFunction());

        // TODO 2.5 Transform: Keyby KeySelector 分流
        // 通过 位置索引 或 字段名称指定分流字段时，返回 类中有类型，但是这个类型是可以人为改变的，无法确定，所以会返回 Tuple，后续使用key的时候，很麻烦
        sensorDS.keyBy(0);
        KeyedStream<WaterSensor, Tuple> sensorKSByFieldName = sensorDS.keyBy("id");
        // 通过 对象.方法 指定 key 的方式， 因为对象的属性类型是不可变的，可以确定返回类型 => 实现 KeySelector 或 lambda
        // 分组是逻辑上的分组，即 给每个数据打上标签（属于哪个分组），并不是对并行度进行改变
        KeyedStream<WaterSensor, String> waterSensorStringKeyedStream = sensorDS.keyBy(r -> r.getId());
        KeyedStream<WaterSensor, String> sensorKSByKeySelector = sensorDS.keyBy(new MyKeySelector());

        // TODO 2.6 Transform: shuffle 数据随机打散
        inputDS.shuffle();

        // TODO 2.7 Transform: Split OutputSelector 水位低于 50 正常，水位 [50，80) 警告， 水位高于 80 告警 ，不是真正的把流分开
        SplitStream<WaterSensor> splitSS = sensorDS.split(new mySplitFunction());

        // TODO 2.8 Transform: select 通过之前的标签名，获取对应的流 一个流可以起多个名字，取出的时候，给定一个名字就行
        splitSS.select("normal");
//        splitSS.select("hi").print("normal");
        splitSS.select("happy");
//        splitSS.select("warn").print("warn");
//        splitSS.select("alarm").print("alarm");

        // TODO 2.9 Transform: connect连接两条流 , 数据类型可以不一样 ,并没有真正合为一个流，后续数据分开处理
        ConnectedStreams<WaterSensor, Integer> sensorNumCS = sensorDS.connect(numDS0);
        // 调用其他算子
        SingleOutputStreamOperator<Object> resultDS = sensorNumCS.map(new MyCoMapFuntion());

        // TODO 2.10 Transform: Union连接多条流 , 数据类型要相同，所有流合并为一个流

        DataStream<Integer> unionDS = numDS0.union(numDS1).union(numDS2);

        unionDS.map(new MapFunction<Integer, Integer>() {
                    @Override
                    public Integer map(Integer value) throws Exception {
                        return value * 10;
                    }
                });

        // TODO 2.11 Operator: 滚动聚合算子：sum，max，min，来一条，聚合一条，输出一次
        // 3.按照 id 分组
        // TODO            数据类型                 key类型
        KeyedStream<Tuple3<String, Long, Integer>, String> sensorKS = sensorDS
                // TODO         传参类型         返回值类型
                .map((MapFunction<WaterSensor, Tuple3<String, Long, Integer>>) value -> new Tuple3<>(value.getId(), value.getTs(), value.getVc()))
                .keyBy( r -> r.f0);
//        sensorKS.sum(2).print("sum");
        sensorKS.max(2).print("max");
//        sensorKS.min(2).print("min");

        // TODO 2.12 Operator: Reduce
        // 1.输入的类型要一致，输出的类型也要一致
        // 2.第一条来的数据，不执行reduceFunction
        // 3.flink帮我们保存了中间状态
        sensorKS.reduce((ReduceFunction<Tuple3<String, Long, Integer>>) (value1, value2) -> Tuple3.of("aaa", 123L, value1.f2 + value2.f2));

        // TODO 2.13 Operator: Process 可以获取到一些 环境信息
        sensorKS.process(
                new KeyedProcessFunction<String, Tuple3<String, Long, Integer>, String>() {
                    /**
                     * 处理数据的方法：来一条处理一条
                     * @param value 输入一条数据
                     * @param ctx   上下文
                     * @param out   采集器
                     * @throws Exception
                     */
                    @Override
                    public void processElement(Tuple3<String, Long, Integer> value, Context ctx, Collector<String> out)   {
                        out.collect("当前key=" + ctx.getCurrentKey() + "当前时间=" + ctx.timestamp() + ",数据=" + value);
                    }
                }
        );

        // 3.打印
        sensorDS.print();
        richSensorDS.print();
        // 4.执行
        env.execute();
    }

    /**
     * 实现MapFunction，指定输入的类型，返回的类型
     * 重写 map方法
     */
    public static class MyMapFunction implements MapFunction<String, WaterSensor> {

        @Override
        public WaterSensor map(String value)   {
            String[] datas = value.split(",");
            return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
        }
    }

    /**
     * 继承 RichMapFunction，指定输入的类型，返回的类型
     * 提供了 open()和 close() 生命周期管理方法
     * 能够获取 运行时上下文对象 =》 可以获取 状态、任务信息 等环境信息
     * 可用于创建连接时的初始化和关闭资源
     */
    public static class MyRichMapFunction extends RichMapFunction<String, WaterSensor> {
        @Override
        public WaterSensor map(String value)   {
            String[] datas = value.split(",");
            return new WaterSensor(getRuntimeContext().getTaskName() + datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
        }
        @Override
        public void open(Configuration parameters)  {
            System.out.println("open...");
        }
        @Override
        public void close() throws Exception {
            System.out.println("close...");
        }
    }

    public static class MyFilterFunction implements FilterFunction<Integer> {

        @Override
        public boolean filter(Integer value)   {
            return value % 2 == 0;
        }
    }

    public static class MyKeySelector implements KeySelector<WaterSensor, String> {

        @Override
        public String getKey(WaterSensor value) throws Exception {
            return value.getId();
        }
    }
    // 每行数据的返回值类型为迭代器，说明标签可以打多个，select时所有包含目标标签的数据都会命中
    public static class mySplitFunction implements  OutputSelector<WaterSensor>{
        @Override
        public Iterable<String> select(WaterSensor value) {
            if (value.getVc() < 50) {
                return Arrays.asList("normal","happy");
            } else if (value.getVc() < 80) {
                return Arrays.asList("warn");
            } else {
                return Arrays.asList("alarm");
            }
        }
    }
    // ConnectedStreams数据分开处理 CoMapFunction ，map1,map2
    public static class MyCoMapFuntion implements CoMapFunction<WaterSensor,Integer,Object>{

        @Override
        public Object map1(WaterSensor value) throws Exception {
            return value.toString();
        }

        @Override
        public Object map2(Integer value) throws Exception {
            return value+10;
        }
    }
}
