package com.cbcxy.flink.apiexample;


import com.cbcxy.flink.bean.UserBehavior;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 需求分析：	网站总浏览量（PV）的统计
 * 字段：用户ID、商品ID、商品类目ID、行为类型和时间戳：userId，itemId，categoryId，behavior，timestamp
 * 行为类型：点击、购买、收藏、喜欢
 */

public class Flink_Page_Visitor {
    public static void main(String[] args) throws Exception {

        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> fileDSS = env.readTextFile("input/UserBehavior.csv");
        // 1.从文件读取数据、转换成 bean对象
        SingleOutputStreamOperator<UserBehavior> userBehaviorDS = fileDSS.map((MapFunction<String, UserBehavior>) value -> {
            String[] datas = value.split(",");
            return new UserBehavior(
                    Long.valueOf(datas[0]),
                    Long.valueOf(datas[1]),
                    Integer.valueOf(datas[2]),
                    datas[3],
                    Long.valueOf(datas[4])
            );
        });
        // 2.处理数据
        // 过滤：只要pv
        SingleOutputStreamOperator<UserBehavior> userBehaviorFilter = userBehaviorDS.filter(data -> "pv".equals(data.getBehavior()));

        // TODO 2.1 参考WordCount思路，实现 PV的统计
        //  转二元组： (pv,1)
       userBehaviorFilter
               .map((MapFunction<UserBehavior, Tuple2<String, Integer>>) value -> Tuple2.of("pv", 1))
               .keyBy(0)
               .sum(1);

        // TODO 2.2 通过process方法实现统计
        // 求和 => 实现 计数 的功能，没有count这种聚合算子，一般找不到现成的算子，那就调用底层的 process
        userBehaviorFilter
                .keyBy(data -> data.getBehavior())
                .process(
                new KeyedProcessFunction<String, UserBehavior, Long>() {
                    // 定义一个变量，来统计条数
                    private Long pvCount = 0L;
                    @Override
                    public void processElement(UserBehavior value, Context ctx, Collector<Long> out) throws Exception {
                        pvCount++;
                        // 采集器往下游发送统计结果
                        out.collect(pvCount);
                    }
                }
        );
        // TODO 2.2 通过FlatMapFunction方法实现
        // 特点：更简洁
        fileDSS.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (value, out) -> {
                    String[] datas = value.split(",");
                    if ("pv".equals(datas[3])) {
                        out.collect(Tuple2.of("pv", 1));
                    }
                })
                .keyBy(0)
                .sum(1);

        env.execute();
    }

}
