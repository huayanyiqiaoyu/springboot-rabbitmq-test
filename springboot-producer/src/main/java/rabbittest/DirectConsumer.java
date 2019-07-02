package rabbittest;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.io.IOException;

import static rabbittest.RabbitMQConfig.QUEUE_A;


@Component
public class DirectConsumer {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());


    @RabbitListener(queues = QUEUE_A)
    public void receive1(String mes, Channel channel) throws InterruptedException, IOException {
        Thread.sleep(1000);
//        //限制发送给同一个消费者不能超过1条信息
//        channel.basicQos(1);
//        final Consumer consumer = new DefaultConsumer(channel){
//            @Override
//            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
////  basicAck 方法的第二个参数 multiple 取值为 false 时，表示通知 RabbitMQ 当前消息被确认；如果为 true，则额外将比第一个参数指定的 delivery tag 小的消息一并确认。（在RabbitMQ的每个信道中，每条消息的 Delivery Tag 从 1 开始递增，这里的批量处理是指同一信道中的消息）：
//                channel.basicAck(envelope.getDeliveryTag(),false);
////                super.handleDelivery(consumerTag, envelope, properties, body);
//            }
//        };
//        //自动确认改为false
//        channel.basicConsume(QUEUE_A,false,consumer);
        logger.info("receive1队列A的消息内容:"+mes);
    }

    @RabbitListener(queues = QUEUE_A)
    public void receive2(String mes,Channel channel) throws IOException, InterruptedException {
        Thread.sleep(2000);
        //限制发送给同一个消费者不能超过1条信息
//        channel.basicQos(3);
//        final Consumer consumer = new DefaultConsumer(channel){
//            @Override
//            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
////  basicAck 方法的第二个参数 multiple 取值为 false 时，表示通知 RabbitMQ 当前消息被确认；如果为 true，则额外将比第一个参数指定的 delivery tag 小的消息一并确认。（在RabbitMQ的每个信道中，每条消息的 Delivery Tag 从 1 开始递增，这里的批量处理是指同一信道中的消息）：
//                channel.basicAck(envelope.getDeliveryTag(),false);
////                super.handleDelivery(consumerTag, envelope, properties, body);
//            }
//        };
//        //自动确认改为false
//        channel.basicConsume(QUEUE_A,true,consumer);
        logger.info("receive2队列A的消息内容:"+mes);
    }

    @RabbitListener(queues = "queueB")
    public void receiveTopic1(String mes){
        logger.info("receiveTopic1队列mes的消息内容:"+mes);
    }

    @RabbitListener(queues = "queueB")
    public void receiveTopic2(String mes){
        logger.info("receiveTopic2队列mes的消息内容:"+mes);
    }

}