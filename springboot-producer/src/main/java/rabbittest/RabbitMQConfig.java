package rabbittest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.ContentTypeDelegatingMessageConverter;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Scope;

/**
 * Broker:它提供一种传输服务,它的角色就是维护一条从生产者到消费者的路线，保证数据能按照指定的方式进行传输,
 *  Exchange：消息交换机,它指定消息按什么规则,路由到哪个队列。
 *  Queue:消息的载体,每个消息都会被投到一个或多个队列。
 *  Binding:绑定，它的作用就是把exchange和queue按照路由规则绑定起来.
 *  Routing Key:路由关键字,exchange根据这个关键字进行消息投递。
 *  vhost:虚拟主机,一个broker里可以有多个vhost，用作不同用户的权限分离。
 *  Producer:消息生产者,就是投递消息的程序.
 *  Consumer:消息消费者,就是接受消息的程序.
 *  Channel:消息通道,在客户端的每个连接里,可建立多个channel.
 */
@Configuration
public class RabbitMQConfig {

    private final Logger logger = LoggerFactory.getLogger(RabbitMQConfig.class);

    @Value("${spring.rabbitmq.host}")
    private String host;

    @Value("${spring.rabbitmq.port}")
    private int port;

    @Value("${spring.rabbitmq.username}")
    private String username;

    @Value("${spring.rabbitmq.password}")
    private String password;

    public final static String EXCHANGE_A = "exchange_a";
    public final static String EXCHANGE_B = "exchange_b";
    public final static String EXCHANGE_C = "exchange_c";

    public final static String QUEUE_A = "queue_a";
    public final static String QUEUE_B = "queue_b";
    public final static String QUEUE_C = "queue_c";

    public final static String ROUTING_A = "routing_a";
    public final static String ROUTING_B = "routing_b";
    public final static String ROUTING_C = "routing_c";

    @Bean
    public ConnectionFactory connectionFactory(){
        CachingConnectionFactory connectionFactory = new CachingConnectionFactory(host, port);
        connectionFactory.setUsername(username);
        connectionFactory.setPassword(password);
        connectionFactory.setVirtualHost("/");

        //在这个位置配置模式是全局的,导致所有消息都开启消息确认模式
        //设置消息回调,发送确认(开启发布确认模式)
        connectionFactory.setPublisherConfirms(true);
        //开启发送失败退回(开启消息发布失败返回模式)
        connectionFactory.setPublisherReturns(true);
        return connectionFactory;
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)//配置成多例,每次都创建一个新的实例
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory,MessageConverter messageConverter){
        RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
        //Mandatory设置为true时,当没有匹配到队列时会自动丢弃消息
        rabbitTemplate.setMandatory(true);
        rabbitTemplate.setMessageConverter(messageConverter);
        return rabbitTemplate;
    }

    //yml中的签收模式失效，被注解注入的SimpleRabbitListenerContainerFactory覆盖，而它默认使用了自动签收。但是消费消息的时候又手动进行channel.basicAck(deliveryTag, false)，于是导致了两次ack,所以报错。
    //解决方法是在rabbitmq的factory中指定ack模式。(factory.setAcknowledgeMode(AcknowledgeMode.MANUAL);)
    @Bean
    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(ConnectionFactory connectionFactory, MessageConverter messageConverter) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        //设置手动确认模式
        factory.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        factory.setConnectionFactory(connectionFactory);
        factory.setMessageConverter(messageConverter);
        return factory;
    }

    @Bean
    public MessageConverter messageConverter() {
        //序列化为json格式
        return new ContentTypeDelegatingMessageConverter(new Jackson2JsonMessageConverter());
    }

    /**
     *  针对消费者配置
     *  1. 设置交换机类型
     *  2. 将队列绑定到交换机
     *  FanoutExchange: 将消息分发到所有的绑定队列，无routingkey的概念
     *  HeadersExchange ：通过添加属性key-value匹配
     *  DirectExchange:按照routingkey分发到指定队列
     *  TopicExchange:多关键字匹配
     */
    @Bean
    public DirectExchange directExchange(){
        return new DirectExchange(EXCHANGE_A);
    }

    @Bean
    public Queue queueA(){
        //持久化
        return new Queue(QUEUE_A,true);
    }

    @Bean
    public Queue queueB(){
        //持久化
        return new Queue("queueB",false,false,false,null);
    }

    @Bean
    public Binding binding2(Queue queueA,DirectExchange directExchange){
        return BindingBuilder.bind(queueA).to(directExchange).with(ROUTING_A);
    }

    @Bean
    public Binding binding5(Queue queueB,DirectExchange directExchange){
        return BindingBuilder.bind(queueB).to(directExchange).with(ROUTING_A);
    }

    //主题交换机
    @Bean
    @Primary
    public TopicExchange topicExchange1(){
        return new TopicExchange(EXCHANGE_B);
    }

    //主题队列
    @Bean(name = "queueTopic1")
    public Queue queueTopic1(){
        return new Queue("queue.mes");
    }

    //主题队列
    @Bean(name = "queueTopic2")
    public Queue queueTopic2(){
        return new Queue("queue.aa");
    }

    @Bean
    public Binding binding1(Queue queueTopic1,TopicExchange topicExchange1){
        return BindingBuilder.bind(queueTopic1).to(topicExchange1).with("queue.#");
    }

    @Bean
    public Binding binding(Queue queueTopic2,TopicExchange topicExchange1){
        return BindingBuilder.bind(queueTopic2).to(topicExchange1).with("queue.aa");
    }

    //扇形队列,不需要routingkey
    @Bean
    public Queue fanoutA(){
        return new Queue("fanoutA");
    }
    @Bean
    public Queue fanoutB(){
        return new Queue("fanoutB");
    }

    //扇形交换机
    @Bean
    public FanoutExchange fanoutExchange(){
        return new FanoutExchange(EXCHANGE_C);
    }

    @Bean
    public Binding binding3(Queue fanoutA,FanoutExchange fanoutExchange){
        return BindingBuilder.bind(fanoutA).to(fanoutExchange);
    }

    @Bean
    public Binding binding4(Queue fanoutB,FanoutExchange fanoutExchange){
        return BindingBuilder.bind(fanoutB).to(fanoutExchange);
    }


}