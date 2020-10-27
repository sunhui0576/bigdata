package com.cbcxy.flink.apiexample;


import com.cbcxy.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.*;


public class Flink_Source {
    public static void main(String[] args) throws Exception {
        // 0.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO 1.1 Collection Source
        DataStreamSource<WaterSensor> sensorDS = env.fromCollection(
                Arrays.asList(
                        new WaterSensor("sensor_1", 15321312412L, 41),
                        new WaterSensor("sensor_2", 15321763412L, 47),
                        new WaterSensor("sensor_3", 15369732412L, 49)
                )
        );

        // TODO 1.2 File Source
        DataStreamSource<String> fileDS = env.readTextFile("input/word.txt");

        // TODO 1.3 Kafka Source
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        DataStreamSource<String> kafkaDS = env.addSource(
                new FlinkKafkaConsumer011<String>(
                        "sensor0421",
                        new SimpleStringSchema(),
                        properties)
        );
        // TODO 1.4 自定义 Source
        DataStreamSource<WaterSensor> inputDS = env.addSource(new MySourceFunction());
        // 2.打印
//        sensorDS.print();
//        fileDS.print();
//        kafkaDS.print();
        inputDS.print();
        // 3.执行
        env.execute();

    }


    public static class MySourceFunction implements SourceFunction<WaterSensor> {
        // 定义一个标志位，控制数据的产生
        private boolean flag = true;

        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {
            Random random = new Random();
            while (flag) {
                ctx.collect(
                        new WaterSensor(
                                "sensor_" + random.nextInt(3),
                                System.currentTimeMillis(),
                                random.nextInt(10) + 40
                        )
                );
                Thread.sleep(2000L);
            }
        }

        @Override
        public void cancel() {
            this.flag = false;
        }
    }
}
