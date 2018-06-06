package com.rocket.demo.producer;

import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.MQProducer;
import com.alibaba.rocketmq.client.producer.MessageQueueSelector;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * User: Park
 * Date: 2018/6/6
 * Description:
 */
public class OrderProducer {

    private static final Logger logger = LoggerFactory.getLogger(OrderProducer.class);

    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("order_msg_group");
        producer.setNamesrvAddr("localhost:9876");
        producer.setInstanceName("testProducer");
        producer.start();

        String[] tags = new String[]{"TA", "TB", "TC", "TD", "TE"};

        for (int i = 0; i < 100; i++) {
            int orderId = i%5;
            Message msg = new Message("Topic-Order-Test", tags[i % tags.length], ("hello "+tags[i % tags.length] + i).getBytes());

            SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                    Integer id = (Integer)o;
                    int index = id % list.size();
                    return list.get(index);
                }
            }, orderId);

            System.out.println(sendResult.getMsgId());

        }
    }

}