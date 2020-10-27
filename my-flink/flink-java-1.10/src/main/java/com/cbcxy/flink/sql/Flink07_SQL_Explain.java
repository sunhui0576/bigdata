package com.cbcxy.flink.sql;


import com.cbcxy.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/9/27 9:34
 */
public class Flink07_SQL_Explain {
    public static void main(String[] args) throws Exception {
        // 0 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 1. 读取数据、转换
        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .readTextFile("input/sensor-data.log")
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));

                    }
                })
                .assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<WaterSensor>() {
                            @Override
                            public long extractAscendingTimestamp(WaterSensor element) {
                                return element.getTs() * 1000L;
                            }
                        }
                );


        // 1. 创建 表执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useOldPlanner() // 使用官方的 planner
//                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 2. 把 DataStream 转换成 Table
        Table sensorTable = tableEnv.fromDataStream(sensorDS, "id,ts as timestamp,vc");

        // 3. 使用 Table API进行处理
        Table resultTable = sensorTable
                .groupBy("id")
//                .aggregate("count(id) as cnt")
                .select("id,count(id)");

        // TODO Explain的使用
        String explain = tableEnv.explain(resultTable);
        System.out.println(explain);


        env.execute();
    }
}
