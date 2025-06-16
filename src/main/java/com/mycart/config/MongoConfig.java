package com.mycart.config;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.apache.camel.component.mongodb.MongoDbComponent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;

@Configuration
public class MongoConfig {

    @Bean(name = "mongoClient")
    public MongoClient mongoClient() {
        return MongoClients.create("mongodb://localhost:27017");
    }

    @Bean
    public MongoTemplate mongoTemplate(MongoClient mongoClient) {
        return new MongoTemplate(mongoClient, "cart");
    }

    @Bean(name = "mongoClient")
    public MongoDbComponent mongoDbComponent(MongoClient mongoClient) {
        MongoDbComponent component = new MongoDbComponent();
        component.setMongoConnection(mongoClient);
        return component;
    }
}