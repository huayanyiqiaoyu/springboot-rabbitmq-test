package rabbittest;

import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.io.IOException;


@Component
public class FanoutConsumer {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());


    //支持自动声明绑定，声明之后自动监听队列的队列，此时@RabbitListener注解的queue和bindings不能同时指定，否则报错

    //fanout扇形交换机与routingkey无关,只要绑定了该交换机即可收到消息
//    @RabbitListener(bindings = {@QueueBinding(value = @Queue(value = "fanoutA"),exchange = @Exchange(value = RabbitMQConfig.EXCHANGE_C))})
//    @RabbitListener(queues = "fanoutA")
//    public void receiveFanout1(String mes){
//        logger.info("receiveFanout1队列mes的消息内容:"+mes);
//    }

//    @RabbitListener(bindings = {@QueueBinding(value = @Queue(value = "fanoutB"),exchange = @Exchange(value = RabbitMQConfig.EXCHANGE_C))})
//    @RabbitListener(queues = "fanoutB")
//    public void receiveFanout2(String mes){
//        logger.info("receiveFanout2队列mes的消息内容:"+mes);
//
//    }

    @RabbitListener(queues = "fanoutA")
    public void receiveFanoutAck1(String mes, Channel channel, Message message) throws IOException {
        logger.info("receiveFanoutAck1队列mes的消息内容:"+mes);
        try {
//            int a = 1;
//            int c = a/0;
            channel.basicAck(message.getMessageProperties().getDeliveryTag(),false);
            logger.info("fanoutA接收消息成功!!!");
        }catch (Exception e){
            e.printStackTrace();
            //丢弃这条消息
            channel.basicNack(message.getMessageProperties().getDeliveryTag(),false,false);
            logger.info("fanoutA接收异常!!!");
        }
    }

    @RabbitListener(queues = "fanoutB")
    public void receiveFanoutAck2(String mes, Channel channel, Message message) throws IOException {
        logger.info("receiveFanoutAck2队列mes的消息内容:"+mes);
        try {
//            int a = 1;
//            int c = a/0;
            channel.basicAck(message.getMessageProperties().getDeliveryTag(),false);
            logger.info("fanoutB接收消息成功!!!");
        }catch (Exception e){
            e.printStackTrace();
            //丢弃这条消息
            channel.basicNack(message.getMessageProperties().getDeliveryTag(),false,false);
            logger.info("fanoutB接收异常!!!");
        }
    }
}