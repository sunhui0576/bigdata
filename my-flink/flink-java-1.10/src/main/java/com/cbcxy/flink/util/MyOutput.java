package com.cbcxy.flink.util;

import com.cbcxy.flink.bean.WaterSensor;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;

import java.util.Arrays;

public class MyOutput {
    public static void main(String[] args) throws Exception {
        // 0.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);

        // 1.Source:读取数据
        DataStreamSource<WaterSensor> sensorDS = env.fromCollection(
                Arrays.asList(
                        new WaterSensor("sensor_1", 15321312412L, 41),
                        new WaterSensor("sensor_2", 15321763412L, 47),
                        new WaterSensor("sensor_3", 15369732412L, 49)
                )
        );

        StreamingFileSink build = StreamingFileSink.forRowFormat(new Path("output/data"), new SimpleStringEncoder("UTF-8")).build();

        // 2.打印
        sensorDS.addSink(build);
        // 3.执行
        env.execute();
    }
}
