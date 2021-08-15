package com.savaz.kafka;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;


public class KafkaProducerWork {

    private static LocalDateTime time = LocalDateTime.now();


    public static void main(String[] args) throws InterruptedException {
        Logger logger = LoggerFactory.getLogger(KafkaProducerWork.class);

        if (args.length < 4)
            return;
        List<String> temp = Arrays.stream(args).map(x -> x.substring(2)).collect(Collectors.toList());
        String[] arguments = new String [temp.size()];
        temp.toArray(arguments);
        List<String> dictionary = null;
        try {
            dictionary = Files.readAllLines(Paths.get(arguments[3]));
        } catch (IOException e) {
            logger.error("Problem with dictionary file loading");
        }
        String[] speakers = arguments[2].split(",");
        String topic = arguments[1];

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(loadProperties(arguments));

        while (true) {
            if (dictionary != null)
                kafkaProducer.send(generateMessage(dictionary, topic, speakers));

            kafkaProducer.flush();
            Thread.sleep(250);
        }

    }

    public static ProducerRecord<String, String> generateMessage(List<String> dictionary, String topic, String[] speakers) {
        time = time.plus(15, ChronoUnit.SECONDS);

        String value = "{ \n" +
                "\"speaker\" : " + "\""+speakers[(int) (Math.random() * speakers.length)] + "\""+", " +
                "\"time\" : " + "\""+time + "\""+"," +
                "\"word\" : " + "\""+ dictionary.get((int) (Math.random() * dictionary.size())) + "\""+" \n" +
                "} ";
        return new ProducerRecord<>(topic, value);
    }

    public static Properties loadProperties(String[] arguments) {
        Properties properties = new Properties();
        Arrays.asList(arguments[0].split(",")).forEach(x -> properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, x));
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        return properties;
    }
}
