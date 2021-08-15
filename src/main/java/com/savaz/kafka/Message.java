package com.savaz.kafka;

import org.codehaus.jackson.annotate.JsonProperty;

import java.sql.Timestamp;

public class Message {
@JsonProperty
   String speaker;
@JsonProperty
   Timestamp time;
@JsonProperty
   String word;

    public Message() {
    }

    public Message(String speaker, Timestamp time, String word) {
        this.speaker = speaker;
        this.time = time;
        this.word = word;
    }
}
