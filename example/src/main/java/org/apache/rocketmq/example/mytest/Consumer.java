package org.apache.rocketmq.example.mytest;


import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.List;


/**
 * @author caoguangwang
 * @date 2022/5/24 下午3:19
 */
public class Consumer {



        public static void main(String[] args) throws InterruptedException, MQClientException {

            // 实例化消费者
            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("immojie-learn-group");

            // 设置NameServer的地址
            consumer.setNamesrvAddr("192.168.0.111:9876");

            // 订阅一个或者多个Topic，以及Tag来过滤需要消费的消息
            consumer.subscribe("immojie-learn", "TagB");

            // 注册回调实现类来处理从broker拉取回来的消息
            consumer.registerMessageListener(new MessageListenerConcurrently() {
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
//                    try {
//                        Thread.sleep(1000);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                    System.out.printf("%s %s Receive New Messages: %s %n", DateUtil.dateSecond(),Thread.currentThread().getName(), msgs);
//                    System.out.printf("%s %n", DateUtil.dateSecond() );

                    for (MessageExt msg : msgs) {
                        try {
                            System.out.printf("%s %s %n", new Date(),new String(msg.getBody(),"utf-8") );
                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                        }
                    }
                    // 标记该消息已经被成功消费
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });
            // 启动消费者实例
            consumer.start();
            System.out.printf("Consumer Started.%n");
        }

}
