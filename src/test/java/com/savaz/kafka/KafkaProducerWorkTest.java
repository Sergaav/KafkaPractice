package com.savaz.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;


class KafkaProducerWorkTest {


    @Test
    void main() {
        MockProducer<String, String> mockProducer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        String message= "{ \n" +
                "\"speaker\":" + "Speaker" + ", \n" +
                "\"time\": " + "time" + "," +
                "\"word\": " + "dictionary" + " \n" +
                "} ";
        mockProducer.send(new ProducerRecord<>("",message));

        Assertions.assertEquals(1, mockProducer.history().size());
    }


    @Test
    void generateMessage() {
        List<String> dictionary = null;
        try {
            dictionary = Files.readAllLines(Paths.get("/home/serha/IdeaProjects/KafkaPractice/TestDict.txt"));
        } catch (IOException e) {
            System.err.println(e);
        }
        String[] speaker = {"Speaker"};
        ProducerRecord<String, String> testProducer = KafkaProducerWork.generateMessage(dictionary, "test", speaker);
        String time = testProducer.value().split(",")[1].split(":",2)[1].trim();
        String expected = "{ \n" +
                "\"speaker\":" + speaker[0] + ", \n" +
                "\"time\": " + time + "," +
                "\"word\": " + dictionary.get(0) + " \n" +
                "} ";
        Assertions.assertEquals(expected, testProducer.value());
    }
}