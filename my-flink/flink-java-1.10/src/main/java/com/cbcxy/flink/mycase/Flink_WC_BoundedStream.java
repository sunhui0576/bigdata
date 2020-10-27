package com.cbcxy.flink.mycase;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink_WC_BoundedStream {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 8888);
        // 处理数据

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = socketDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public void flatMap(String in, Collector<Tuple2<String, Integer>> out) throws Exception {
                // 切分
                String[] words = in.split(" ");
                // 转二元组
                for (String word : words) {
                    out.collect(new Tuple2(word, 1));
                    // 或 out.collect(Tuple2.of(word, 1));
                }
            }
        });
        // 分流，聚合，输出
        wordAndOne.keyBy(0).sum(1).print();
        // 启动
        env.execute();
    }
}
