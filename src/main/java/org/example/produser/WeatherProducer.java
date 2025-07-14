package org.example.produser;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.service.WeatherService;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class WeatherProducer {



    private static final Logger logger = LoggerFactory.getLogger(WeatherProducer.class);


    public static void main(String[] args) throws InterruptedException {
        WeatherService weatherService = new WeatherService();
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(props);
        String topic = "weather-topic";

        Random random = new Random();

        while (true) {

            JSONObject weatherData = weatherService.GenerateMessage();


            // Отправляем сообщение в Kafka
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, weatherData.toString());
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    logger.error("Error sending message", exception);
                } else {
                    logger.info("Sent message to topic: {}, partition: {}, offset: {}", topic, metadata.partition(), metadata.offset());
                }
            });

            // Пауза на 2 секунды
            TimeUnit.SECONDS.sleep(2);
        }

        // producer.close();
    }
}