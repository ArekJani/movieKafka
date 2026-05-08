package kafka;

import java.lang.System;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.time.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

public class KafkaExample {
    public static void main(String[] args) {
        try {
            String topic = "topic_Arka";
            final Properties config = readConfig("C:\\Users\\janic\\IdeaProjects\\movies\\src\\main\\resources\\client.properties");

            produce(topic, config);
            consume(topic, config);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Properties readConfig(final String configFile) throws IOException {
        // reads the client configuration from client.properties
        // and returns it as a Properties object
        if (!Files.exists(Paths.get(configFile))) {
            throw new IOException(configFile + " not found.");
        }

        final Properties config = new Properties();
        try (InputStream inputStream = new FileInputStream(configFile)) {
            config.load(inputStream);
        }

        return config;
    }

    public static void produce(String topic, Properties config) {
        // sets the message serializers
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());

        // creates a new producer instance and sends a sample message to the topic
        String key = "klucz";
        KafkaItem item = new KafkaItem("Arka", 5);
        Producer<String, KafkaItem> producer = new KafkaProducer<>(config);
        producer.send(new ProducerRecord<>(topic, key, item));
        item.setRating(4);
        System.out.printf(
                "Produced message to topic %s: key = %s value = %s%n", topic, key, item.getName() + " " + item.getRating());

        producer.send(new ProducerRecord<>(topic, key+1, item), (recordMetadata, e) -> {
            if (e != null) {
                System.err.printf("Failed to produce message to topic %s: %s%n", topic, e.getMessage());
            } else {
                System.out.printf(
                        "Produced message to topic %s: key = %s value = %s%n", topic, key, item.getName() + " " + item.getRating());
            }
        });
        // closes the producer connection
        producer.close();
    }

    public static void consume(String topic, Properties config) {
        // sets the group ID, offset and message deserializers
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "java-group-1");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeSerializer.class.getName());
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");   //TRUE-ponownie zaciaga wszystkie msg z topica, FALSE-tylko ostatnie, które nie zostały zaciągnięte

        // creates a new consumer instance and subscribes to messages from the topic
        KafkaConsumer<String, KafkaItem> consumer = new KafkaConsumer<>(config);
        consumer.subscribe(List.of(topic));
        consumer.subscription().forEach(t -> System.out.println("Subscribed to topic: " + t));

        while (true) {
            // polls the consumer for new messages and prints them
            ConsumerRecords<String, KafkaItem> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, KafkaItem> record : records) {
                System.out.printf(
                        "Consumed message from topic %s: key = %s value = %s offset = %d partition = %d%n ", topic, record.key(), record.value(), record.offset(), record.partition());
            }

//            consumer.commitAsync((offsets, exception) -> {
//                if (exception != null) {
//                    System.err.printf("Failed to commit offsets: %s%n", exception.getMessage());
//                } else {
//                    System.out.printf("Offsets committed successfully: %s%n", offsets);
//                }
//            });
        }


    }
}

