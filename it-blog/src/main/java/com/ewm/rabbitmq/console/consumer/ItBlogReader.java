package com.ewm.rabbitmq.console.consumer;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

public class ItBlogReader {
    public static final String EXCHANGE_NAME = "it_blog_exchanger";
    public static final String SUBSCRIBE_ON_TOPIC_COMMAND = "set_topic";
    public static final String UNSUBSCRIBE_FROM_TOPIC_COMMAND = "remove_topic";
    private final static String QUIT_COMMAND = "quit";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
        String queueName = channel.queueDeclare().getQueue();

        while(true){

            Scanner scanner = new Scanner(System.in);
            String input = scanner.nextLine();

            if(input.startsWith(QUIT_COMMAND)){
                return;
            } else if(input.startsWith(SUBSCRIBE_ON_TOPIC_COMMAND + "\s")){
                String topic = input.substring((SUBSCRIBE_ON_TOPIC_COMMAND + "\s").length());
                channel.queueBind(queueName, EXCHANGE_NAME, topic);

                DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                    String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                    System.out.printf("%s: %s%n", consumerTag, message);
                };

                channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {});
            } else if(input.startsWith(UNSUBSCRIBE_FROM_TOPIC_COMMAND + "\s")){
                String topic = input.substring((UNSUBSCRIBE_FROM_TOPIC_COMMAND + "\s").length());
                channel.queueUnbind(queueName, EXCHANGE_NAME, topic);
            } else {
                System.out.println("Invalid command");
                continue;
            }

        }

    }
}
