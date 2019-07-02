package rabbittest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@Component
@RestController
public class Producer {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private AmqpTemplate amqpTemplate;

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private ApplicationContext applicationContext;

    @GetMapping(value = "/a")
    public void send1(){
//        CorrelationData correlationData = new CorrelationData();
        //通过交换机和路由key将消息绑定到对应的queue上
        for(int a = 1;a<11;a++){
            amqpTemplate.convertAndSend(RabbitMQConfig.EXCHANGE_A,RabbitMQConfig.ROUTING_A,"数据"+a);
            logger.info("send1:"+"数据"+a);
        }

    }

    @GetMapping(value = "/b")
    public void send2(){

        //通过交换机和路由key将消息绑定到对应的queue上
        for(int a = 1;a<11;a++){

//            CorrelationData correlationData = new CorrelationData(a+"");
            amqpTemplate.convertAndSend(RabbitMQConfig.EXCHANGE_A,RabbitMQConfig.ROUTING_A,"数据"+a);
            logger.info("send2:"+"数据"+a);
        }

    }

    @GetMapping(value = "/c")
    public void sendTopic(){

        //通过交换机和路由key将消息绑定到对应的queue上
        for(int a = 1;a<6;a++){
            CorrelationData correlationData = new CorrelationData(a+"");
            amqpTemplate.convertAndSend(RabbitMQConfig.EXCHANGE_B,"queue.mes","数据"+correlationData);
            logger.info("sendTopic:queue.mes"+"数据"+a);

            amqpTemplate.convertAndSend(RabbitMQConfig.EXCHANGE_B,"queue.aa","数据"+correlationData);
            logger.info("sendTopic:queue.aa"+"数据"+a);
        }

    }

    /**
     * 每次获取新的RabbitTemplate
     * @return
     */
    private RabbitTemplate getRabbitTemplate(){
        RabbitTemplate rabbitTemplate = (RabbitTemplate) applicationContext.getBean("rabbitTemplate");
        logger.info("获取新的RabbitTemplate:{}",rabbitTemplate );
        return rabbitTemplate;
    }

    @GetMapping(value = "/d")
    public void sendFanout(){
//        CorrelationData correlationData = new CorrelationData();
        //通过交换机和路由key将消息绑定到对应的queue上
        for(int a = 1;a<5;a++){
            amqpTemplate.convertAndSend(RabbitMQConfig.EXCHANGE_C,"aa","数据"+a);
            logger.info("sendTopic:QUEUE_A"+"扇形交换机"+a);
        }
    }

        //rabbitmq默认机制是轮询分发,即每个消费者接收均衡的信息


//    I:NONE：默认为NONE，也就是自动ack模式，在消费者接受到消息后无需手动ack，消费者会自动将消息ack掉。
//    II:MANUAL：即为手动ack模式，消费者在接收到消息后需要手动ack消息，不然消息将一直处于uncheck状态，在应用下次启动的时候会再次对消息进行消费。使用该配置需要注意的是，配置开启后即项目全局开启手动ack模式，所有的消费者都需要在消费信息后手动ack消息，否则在重启应用的时候将会有大量的消息无法被消费掉而重复消费。
//    III:AUTO：自动确认ack 如果此时消费者抛出异常，不同的异常会有不同的处理方式。
    @GetMapping(value = "/ackSend")
    public void ackSend(){
        //发布确认机制
        //生产者发送消息,绑定exchange没有对应的queue的消息将会退回
        //因为在开启了setReturnCallback,setConfirmCallback模式后,只能设置一次,否则会报异常(only one confirmcallback...),所以采用创建
        //多个RabbitTemplate实例
        RabbitTemplate rabbitTemplate = getRabbitTemplate();
        //发送消息失败时会回调,exchange发送到队列失败(例如:发送到别的交换机上去了)
        rabbitTemplate.setReturnCallback((message, replyCode, replyText, exchange, routingKey) -> {
            logger.info("ackSend发送消息被返回:"+exchange+routingKey);
        });
//        int a = 1;
//        int c = a/0;
//        这里使用了RabbitTemplate而没有使用AmqpTemplate，可以将RabbitTemplate看作一个实现了AmqpTemplate的工具类，其中定义了更多方法供开发者使用。
        //当发送的交换机不存在或者队列不存在时,ack为false
        //ConfirmCallback是代表消息已经发送到了rabbitmq,不是发送失败返回
        rabbitTemplate.setConfirmCallback((correlationData, ack, cause) -> {
            String correlationid = "";
            if(correlationData != null){
                correlationid = correlationData.getId();
            }
            if(!ack){
                logger.info("消息发送失败:"+cause+"=="+correlationid);
            }else{
                logger.info("消息发送成功"+"=="+correlationid);
            }
        });
        String meg = "测试消息ack";
        CorrelationData correlationData = new CorrelationData();
        correlationData.setId(UUID.randomUUID().toString());
        rabbitTemplate.convertAndSend(RabbitMQConfig.EXCHANGE_C,"aa",meg,correlationData);
    }

}