package org.apache.rocketmq.example.mytest;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * @author caoguangwang
 * @date 2022/5/24 下午3:09
 */
public class SyncProducer {

    public static void main(String[] args) throws Exception {
        // 实例化消息生产者Producer
        DefaultMQProducer producer = new DefaultMQProducer("immojie-learn-group");
        // 设置NameServer的地址
        producer.setNamesrvAddr("192.168.0.111:9876");
        // 启动Producer实例
        producer.start();

        //同步发送
        sync(producer);
        //异步发送
//        async(producer);

        // 如果不再发送消息，关闭Producer实例。
        producer.shutdown();
    }

    public static void  sync(DefaultMQProducer producer) throws Exception {

        for (int i = 0; i < 1; i++) {
            // 创建消息，并指定Topic，Tag和消息体
            Message msg = new Message("immojie-learn" /* Topic */,
                    "TagB" /* Tag */,
                    ("Hello RocketMQ-a " + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
            );
            msg.putUserProperty("version","1.0");
            // 发送消息到一个Broker
            SendResult sendResult = producer.send(msg);
            // 通过sendResult返回消息是否成功送达
            System.out.printf("%s%n", sendResult);
        }
    }



    public static void  async(DefaultMQProducer producer) throws Exception {

        for (int i = 0; i < 1; i++) {
            // 创建消息，并指定Topic，Tag和消息体
            Message msg = new Message("immojie-learn" /* Topic */,
                    "TagB" /* Tag */,
                    ("Hello RocketMQ-a " + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
            );
            msg.putUserProperty("version","1.0");
            producer.send(msg, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.printf("OK %s %n",
                            sendResult.getMsgId());
                }
                @Override
                public void onException(Throwable e) {
                    System.out.printf(" Exception %s %n",   e);
                    e.printStackTrace();
                }
            });

        }
    }
}
