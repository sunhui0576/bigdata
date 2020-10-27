package com.cbcxy.flink.apiexample;


import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Flink_Sink {
    public static void main(String[] args) throws Exception {

        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1.从文件读取数据
        DataStreamSource<String> inputDS = env
                .readTextFile("input/sensor-data.log");

        //TODO 2.1 Sink到 Kafka
        inputDS.addSink(new FlinkKafkaProducer011<>("hadoop102:9092", "sensor0421", new SimpleStringSchema()));

        //TODO 2.2 Sink到 Redis
        FlinkJedisPoolConfig jedisConfig = new FlinkJedisPoolConfig.Builder().setHost("hadoop102").setPort(6379).build();
        inputDS.addSink(new RedisSink<String>(jedisConfig, new MyRedisSink()));

        //TODO 2.3 Sink到 ES
        inputDS.addSink(myESSink());
        //TODO 2.4 Sink到 自定义的 MySQL
        inputDS.addSink(new MySQLFunction());

        env.execute();
    }



    //TODO 2.5 Sink到 自定义的 HBase

    //TODO 2.4 Sink到 自定义的 HDFS

    //TODO 2.4 Sink到 自定义的 Hive

    //TODO 2.4 Sink到 自定义的

    public static class MyRedisSink implements RedisMapper<String>{
        // redis 的命令: key是最外层的 key
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET,"sensor0421");
        }

        // Hash类型：这个指定的是 hash 的key
        @Override
        public String getKeyFromData(String data) {
            String[] datas = data.split(",");
            return datas[1];
        }

        // Hash类型：这个指定的是 hash 的 value
        @Override
        public String getValueFromData(String data) {
            String[] datas = data.split(",");
            return datas[2];
        }
    }
    // 因为要连接数据库，所有使用rich版本的sinkFunction来获取open close方法
    public static class MySQLFunction extends RichSinkFunction<String> {
        private Connection conn = null;
        private PreparedStatement pstmt = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test", "root", "000000");
            // 预编译sql,?是占位符
            pstmt = conn.prepareStatement("INSERT INTO sensor VALUES (?,?,?)");
        }

        @Override
        public void close() throws Exception {
            pstmt.close();
            conn.close();
        }

        @Override
        public void invoke(String value, SinkFunction.Context context) throws Exception {
            String[] datas = value.split(",");
            pstmt.setString(1, datas[0]);
            pstmt.setLong(2, Long.valueOf(datas[1]));
            pstmt.setInt(3, Integer.valueOf(datas[2]));
            pstmt.execute();
        }
    }
    // 官方样例
    private static class TestElasticSearchSinkFunction implements ElasticsearchSinkFunction<Tuple2<Integer, String>> {

        public IndexRequest createIndexRequest(Tuple2<Integer, String> element) {
            Map<String, Object> json = new HashMap<>();
            json.put("data", element.f1);

            return Requests.indexRequest()
                    .index("my-index")
                    .type("my-type")
                    .id(element.f0.toString())
                    .source(json);
        }

        public void process(Tuple2<Integer, String> element, RuntimeContext ctx, RequestIndexer indexer) {
            indexer.add(createIndexRequest(element));
        }
    }
    public static ElasticsearchSink myESSink() {
        //es集群地址
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop102",9200));
        httpHosts.add(new HttpHost("hadoop103",9200));
        httpHosts.add(new HttpHost("hadoop104",9200));

        return new ElasticsearchSink
                .Builder(httpHosts,new myESSinkFunction())
                .build();
    }
    public static class myESSinkFunction implements ElasticsearchSinkFunction<String>{
        @Override
        public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
            // 将数据放在Map中
            Map<String, String> dataMap = new HashMap<>();
            dataMap.put("data", element);
            // 创建 IndexRequest =》 指定index，指定type，指定source
            IndexRequest indexRequest = Requests.indexRequest("sensor0421").type("reading").source(dataMap);
            // 添加到 RequestIndexer
            indexer.add(indexRequest);
        }
    }

}
