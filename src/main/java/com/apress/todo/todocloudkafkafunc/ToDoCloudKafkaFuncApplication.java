package com.apress.todo.todocloudkafkafunc;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.context.annotation.ComponentScan;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serde;
import org.springframework.boot.SpringApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;


import com.apress.todo.domain.Order;

//EnableBinding will be Deprecated as of 3.1 in favor of functional programming model, stay tuned for the next tutorials
//@EnableBinding(value = {OrderBinder.class})
@SpringBootApplication
@ComponentScan(basePackages={"com.apress.todo"})
public class ToDoCloudKafkaFuncApplication {

    public final static String STATE_STORE_NAME = "todo-cloud-kafka-funf-order-events";

	public static void main(String[] args) {
        SpringApplication.run(ToDoCloudKafkaFuncApplication.class, args);
    }

    @Bean
    public Serde<Order> orderJsonSerde() {
        return new JsonSerde<>(Order.class, new ObjectMapper());
    }

}


