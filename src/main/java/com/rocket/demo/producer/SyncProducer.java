package com.rocket.demo.producer;

import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User: Park
 * Date: 2018/6/4
 * Description:
 */
public class SyncProducer {

    private static final Logger logger = LoggerFactory.getLogger(SyncProducer.class);

    public static void main(String[] args) throws Exception {

        DefaultMQProducer producer = new DefaultMQProducer("my_group");
        producer.setNamesrvAddr("localhost:9876");

        producer.start();


        for(int i = 0; i< 100; i++){

            Message msg = new Message("TestTopic", "TagA", ("Hello"+i).getBytes());
            SendResult sendResult = producer.send(msg);
            System.out.println(sendResult.getMsgId());
        }
        producer.shutdown();
    }

}