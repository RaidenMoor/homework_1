package org.example.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class WeatherConsumer {

    private static final Logger logger = LoggerFactory.getLogger(WeatherConsumer.class);

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "weather-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("weather-topic"));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    String message = record.value();
                    try {
                        JSONObject weatherData = new JSONObject(message);
                        int temperature = weatherData.getInt("temperature");
                        String condition = weatherData.getString("condition");
                        logger.info("Received weather data: Temperature = {}, Condition = {}", temperature, condition);
                    } catch (Exception e) {
                        logger.error("Error parsing JSON message: {}", message, e);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Consumer error", e);
        } finally {
            consumer.close();
        }
    }
}