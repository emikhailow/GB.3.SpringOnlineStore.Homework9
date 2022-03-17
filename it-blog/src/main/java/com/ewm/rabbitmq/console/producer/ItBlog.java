package com.ewm.rabbitmq.console.producer;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

public class ItBlog {
    private final static String EXCHANGER_NAME = "it_blog_exchanger";
    private final static String QUIT_COMMAND = "quit";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        try(Connection connection = connectionFactory.newConnection();
            Channel channel = connection.createChannel()){
            channel.exchangeDeclare(EXCHANGER_NAME, BuiltinExchangeType.TOPIC);

            Scanner scanner = new Scanner(System.in);

            while(true){

                String input = scanner.nextLine();
                if(input.equals(QUIT_COMMAND)){
                    return;
                }

                String[] strings = input.split("\s");
                if(strings.length <= 1){
                    System.out.println("Invalid input");
                    continue;
                }

                String topic = input.split("\s")[0];
                String message = input.substring((topic + "\s").length());

                channel.basicPublish(EXCHANGER_NAME, topic, null, message.getBytes(StandardCharsets.UTF_8));

            }

        }
    }

}
