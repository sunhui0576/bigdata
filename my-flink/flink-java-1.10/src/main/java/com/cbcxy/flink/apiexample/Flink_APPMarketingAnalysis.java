package com.cbcxy.flink.apiexample;


import com.cbcxy.flink.bean.MarketingUserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;


public class Flink_APPMarketingAnalysis {
    public static void main(String[] args) throws Exception {
        // 0 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1.读取数据、转换成 bean对象
        DataStreamSource<MarketingUserBehavior> appDS = env.addSource(new AppSource());

        // TODO 2.1 处理数据：不同渠道不同行为 的统计
        // 按照 统计的维度 分组 ： 渠道、行为 ，多个维度可以拼接在一起
        // 转换成 （渠道_行为，1）二元组
        appDS.map((MapFunction<MarketingUserBehavior, Tuple2<String, Integer>>) value -> Tuple2.of(value.getChannel() + "_" + value.getBehavior(), 1))
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}))
                .keyBy(data -> data.f0)
                .sum(1);

        // TODO 2.2 处理数据：不同行为 的统计(不分渠道)
        // 按照 统计的维度 分组 ： 行为
        // 转换成 （行为，1）二元组
        appDS.map((MapFunction<MarketingUserBehavior, Tuple2<String, Integer>>) value -> Tuple2.of(value.getBehavior(), 1))
            .returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}))
            .keyBy(data -> data.f0)
            .sum(1);
        env.execute();
    }


    public static class AppSource implements SourceFunction<MarketingUserBehavior> {

        private boolean flag = true;
        private List<String> behaviorList = Arrays.asList("DOWNLOAD", "INSTALL", "UPDATE", "UNINSTALL");
        private List<String> channelList = Arrays.asList("XIAOMI", "HUAWEI", "OPPO", "VIVO");

        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
            while (flag) {
                Random random = new Random();
                ctx.collect(
                        new MarketingUserBehavior(
                                Long.valueOf(random.nextInt(10)),
                                behaviorList.get(random.nextInt(behaviorList.size())),
                                channelList.get(random.nextInt(channelList.size())),
                                System.currentTimeMillis()
                        )
                );
                Thread.sleep(1000L);
            }
        }
        @Override
        public void cancel() {
            flag = false;
        }
    }
}
