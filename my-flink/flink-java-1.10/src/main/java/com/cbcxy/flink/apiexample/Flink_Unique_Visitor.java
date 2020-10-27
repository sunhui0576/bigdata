package com.cbcxy.flink.apiexample;


import com.cbcxy.flink.bean.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

public class Flink_Unique_Visitor {
    public static void main(String[] args) throws Exception {

        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.从文件读取数据、转换成 bean对象
        SingleOutputStreamOperator<UserBehavior> userBehaviorDS = env
                .readTextFile("input/UserBehavior.csv")
                .map((MapFunction<String, UserBehavior>) value -> {
                    String[] datas = value.split(",");
                    return new UserBehavior(
                            Long.valueOf(datas[0]),
                            Long.valueOf(datas[1]),
                            Integer.valueOf(datas[2]),
                            datas[3],
                            Long.valueOf(datas[4])
                    );
                })
                .returns(TypeInformation.of(new TypeHint<UserBehavior>() {}));

        // TODO 实现 UV的统计 ：对 userId进行去重，统计
        // 2.处理数据
        // 2.1 过滤出 pv 行为 => UV 就是 PV的去重，所以行为还是 pv
        SingleOutputStreamOperator<UserBehavior> userBehaviorFilter = userBehaviorDS.filter(data -> "pv".equals(data.getBehavior()));
        // 2.2 转换成二元组 ("uv",userId)
        // => 第一个给 uv，是为了分组，分组是为了调用 sum或 process等方法
        // => 第二个给userId，是为了将 userId存到 Set里
        userBehaviorFilter
                .map((MapFunction<UserBehavior, Tuple2<String, Long>>) value -> Tuple2.of("uv", value.getUserId()))
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {}))
                .keyBy(data -> data.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Long>, Integer>() {
                    // 定义一个Set，用来去重并存放 userId
                    private Set<Long> uvSet = new HashSet<>();
                    @Override
                    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Integer> out) throws Exception {
                        uvSet.add(value.f1);
                        out.collect(uvSet.size());
                    }
                }
        );

        env.execute();
    }

}
