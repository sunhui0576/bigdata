package com.cbcxy.flink.apiexample;


import com.cbcxy.flink.bean.OrderEvent;
import com.cbcxy.flink.bean.TxEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

public class Flink_OrderTxDetect {
    public static void main(String[] args) throws Exception {
        // 0 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1.读取数据，转成bean对象
        SingleOutputStreamOperator<OrderEvent> orderDS = env
                .readTextFile("input/OrderLog.csv")
                .map((MapFunction<String, OrderEvent>) value -> {
                    String[] datas = value.split(",");
                    return new OrderEvent(
                            Long.valueOf(datas[0]),
                            datas[1],
                            datas[2],
                            Long.valueOf(datas[3])
                    );
                })
                .returns(TypeInformation.of(new TypeHint<OrderEvent>() {}));

        SingleOutputStreamOperator<TxEvent> txDS = env
                .readTextFile("input/ReceiptLog.csv")
                .map((MapFunction<String, TxEvent>) value -> {
                    String[] datas = value.split(",");
                    return new TxEvent(
                            datas[0],
                            datas[1],
                            Long.valueOf(datas[2])
                    );
                })
                .returns(TypeInformation.of(new TypeHint<TxEvent>() {}));

        // TODO 2.处理数据：实时对账 监控
        // 对于业务数据和交易数据通过 txId进行connect 需要对乱序数据处理
        // 先keyby再connect
        (orderDS.keyBy(order -> order.getTxId()))
                .connect(txDS.keyBy(tx -> tx.getTxId()))
                .process(new CoProcessFunction<OrderEvent, TxEvent, String>() {
                    // 用来存放 交易系统 的数据
                    private Map<String, TxEvent> txMap = new HashMap<>();
                    // 用来存放 业务系统 的数据
                    private Map<String, OrderEvent> orderMap = new HashMap<>();
                    // 业务数据
                    @Override
                    public void processElement1(OrderEvent value, Context ctx, Collector<String> out) {
                        TxEvent txEvent = txMap.get(value.getTxId());
                        if (txEvent == null) {
                            // 1.说明 交易数据 没来 => 等 ， 把自己临时保存起来
                            orderMap.put(value.getTxId(), value);
                        } else {
                            // 2.交易码不为空 说明 交易数据 来了 => 对账成功并删除对账成功的交易码
                            txMap.remove(value.getTxId());
                        }
                    }
                    // 交易数据
                    @Override
                    public void processElement2(TxEvent value, Context ctx, Collector<String> out)   {
                        OrderEvent orderEvent = orderMap.get(value.getTxId());
                        if (orderEvent == null) {
                            // 1.说明 业务数据 没来 => 把自己 临时保存起来
                            txMap.put(value.getTxId(), value);
                        } else {
                            // 2.交易码不为空 说明 业务数据 来了 => 对账成功并删除对账成功的交易码
                            orderMap.remove(value.getTxId());
                        }
                    }
                }
        );
        
        env.execute();
    }


}
