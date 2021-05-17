package com.abhinavjdwij.poc.kafkaproducer.kafka.config;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConsumerAwareErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.group.id}")
    private String groupId;

    @Bean("GenericConsumerFactory")
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class); // error handler delegates key to this class
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class); // error handler delegates value to this class
        props.put(JsonDeserializer.KEY_DEFAULT_TYPE, String.class); // default key type is String
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, Object.class); // default value type is Object (to maintain single GenericConsumerFactory)
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*"); // allow deserialization of class from any package (not just from [java.io, java.lang])
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false); // since package names are different in consumer and producer microservices
        return new DefaultKafkaConsumerFactory(props);
    }

    @Bean("GenericConcurrentListener")
    public ConcurrentKafkaListenerContainerFactory<String, Object>
    concurrentKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory kafkaListenerFactory =
                new ConcurrentKafkaListenerContainerFactory();
        kafkaListenerFactory.setConsumerFactory(consumerFactory());
        kafkaListenerFactory.setErrorHandler(genericConsumerAwareErrorHandler()); // error handler is mandatory otherwise errors will enter infinite loop (can create massive logs until message expires)
        return kafkaListenerFactory;
    }

    @Bean("GenericConsumerAwareErrorHandler")
    public ConsumerAwareErrorHandler genericConsumerAwareErrorHandler() {
        return new ConsumerAwareErrorHandler() {
            @Override
            public void handle(Exception e, ConsumerRecord<?, ?> consumerRecord, Consumer<?, ?> consumer) {
                System.out.println("GenericConcurrentListener Exception :: " + e.getMessage());
            }

            @Override
            public void handle(Exception thrownException, ConsumerRecord<?, ?> data) {
                System.out.println("GenericConcurrentListener Exception :: " + thrownException.getMessage());
            }

            @Override
            public void handle(Exception thrownException, List<ConsumerRecord<?, ?>> data, Consumer<?, ?> consumer, MessageListenerContainer container) {
                System.out.println("GenericConcurrentListener Exception :: " + thrownException.getMessage());
            }
        };
    }

}
