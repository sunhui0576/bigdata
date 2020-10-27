package com.cvcxy.gmalllogger;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cvcxy.common.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Author lzc
 * @Date 2020/7/13 16:40
 */
/*@Controller
@ResponseBody*/
@RestController  // 等价于 @Controller + @ResponseBody
public class LoggerController {
    // http://localhost:8080/log?log=abc

    @PostMapping("/log")
    public String doLog(String log) {
        //1. 给日志添加一个时间戳
        log = addTS(log);
        // 2. 数据落盘(为离线数据做准备)
        saveToDisk(log);
        // 3. 把数据写入到kafka, 需要写入到topic
        sendToKafka(log);

        return "ok";
    }
    @Autowired
    KafkaTemplate kafka;
    /**
     * 把日志发送到kafka
     * 不同的日志写入到不同的topic
     *
     * @param log
     */
    private void sendToKafka(String log) {
        if (log.contains("\"startup\"")) {
            kafka.send(Constant.STARTUP_TOPIC, log);
        }else{
            kafka.send(Constant.EVENT_TOPIC, log);
        }
    }

    Logger logger = LoggerFactory.getLogger(LoggerController.class);

    /**
     * 把日志写入到磁盘
     *
     * @param log
     */
    private void saveToDisk(String log) {
        logger.info(log);
    }

    /**
     * 给日志添加时间戳
     *
     * @param log
     * @return
     */
    private String addTS(String log) {
        JSONObject obj = JSON.parseObject(log);
        obj.put("ts", System.currentTimeMillis());
        return JSON.toJSONString(obj);
    }

}
