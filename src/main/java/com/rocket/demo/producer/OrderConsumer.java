package com.rocket.demo.producer;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.alibaba.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * User: Park
 * Date: 2018/6/6
 * Description:
 */
public class OrderConsumer {

    private static final Logger logger = LoggerFactory.getLogger(OrderConsumer.class);

    public static void main(String[] args) throws Exception {
        DefaultMQPushConsumer consumer =  new DefaultMQPushConsumer("order_msg_group");
        consumer.setNamesrvAddr("localhost:9876");
        consumer.subscribe("Topic-Order-Test","TA || TB || TC");



        consumer.registerMessageListener(new MessageListenerOrderly() {

            int i = 0;
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext consumeOrderlyContext) {

                for(MessageExt messageExt: list){
                    String content = new String(messageExt.getBody());
                    if(content.contains("TA")){
                        System.out.println(content);
                    }
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });

        consumer.start();
    }

}