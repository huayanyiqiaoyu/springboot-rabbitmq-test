package rabbittest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;


@Component
public class TopicConsumer {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());


    //支持自动声明绑定，声明之后自动监听队列的队列，此时@RabbitListener注解的queue和bindings不能同时指定，否则报错
    //主题交换机使用rabbitListener注解绑定会失败,只能在rabbitmqconfig配置中绑定队列

//    @RabbitListener(bindings = {@QueueBinding(value = @Queue(value = "queue.aa"),exchange = @Exchange(value = RabbitMQConfig.EXCHANGE_B),key = "queue.aa")})
    @RabbitListener(queues = "queue.aa")
    public void receiveTopic(String mes){
        logger.info("receiveTopic队列aa的消息内容:"+mes);
    }

//    @RabbitListener(bindings = {@QueueBinding(value = @Queue(value = "queue.mes"),exchange = @Exchange(value = RabbitMQConfig.EXCHANGE_B),key = "queue.#")})
    @RabbitListener(queues = "queue.mes")
    public void receiveTopic1(String mes){
        logger.info("receiveTopic队列mes的消息内容:"+mes);
    }


}