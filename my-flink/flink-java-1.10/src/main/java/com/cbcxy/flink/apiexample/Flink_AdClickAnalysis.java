package com.cbcxy.flink.apiexample;


import com.cbcxy.flink.bean.AdClickLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink_AdClickAnalysis {
    public static void main(String[] args) throws Exception {
        // 0 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1.读取数据，转成bean对象
        SingleOutputStreamOperator<AdClickLog> adClickDS = env
                .readTextFile("input/AdClickLog.csv")
                .map((MapFunction<String, AdClickLog>) value -> {
                    String[] datas = value.split(",");
                    return new AdClickLog(
                            Long.valueOf(datas[0]),
                            Long.valueOf(datas[1]),
                            datas[2],
                            datas[3],
                            Long.valueOf(datas[4])
                    );
                })
                .returns(TypeInformation.of(new TypeHint<AdClickLog>() {}));

        // 2.处理数据：不同省份、不同广告的点击量 实时统计
        adClickDS.map((MapFunction<AdClickLog, Tuple2<String, Integer>>) value -> Tuple2.of(value.getProvince() + "_" + value.getAdId(), 1))
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}))
                .keyBy(data -> data.f0)
                .sum(1);

        env.execute();
    }


}
