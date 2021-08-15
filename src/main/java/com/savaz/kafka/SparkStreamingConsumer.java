package com.savaz.kafka;


import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.TimestampType;

public class SparkStreamingConsumer {

    private static final String wordToChange = "AAAAAAA";


    public static void main(String[] args) throws TimeoutException, StreamingQueryException, IOException {
        Logger logger = LoggerFactory.getLogger(SparkStreamingConsumer.class);

        if (args.length < 4)
            return;
        List<String> temp = Arrays.stream(args).map(x -> x.substring(2)).collect(Collectors.toList());
        String[] arguments = new String[temp.size()];
        temp.toArray(arguments);
        String bootstrapServers = arguments[0];
        String topic = arguments[1];
        String pathCensoredWords = arguments[2];
        String windowDuration = arguments[3];
        SparkSession session = SparkSession.builder()
                .master("local")
                .appName("StructuredStreaming")
                .getOrCreate();

        session.sparkContext().setLogLevel("ERROR");

        logger.info("Reading from Kafka..");

        Dataset<Row> df = session
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", topic)
                .option("value.deserializer", StringDeserializer.class.getName())
                .load();

        StructType schema = new StructType()
                .add("speaker", StringType)
                .add("time", TimestampType)
                .add("word", StringType);

        List<String> censoredWords = Files.readAllLines(Paths.get(pathCensoredWords));

        Dataset<Row> set = df
                .select(from_json(col("value").cast(StringType), schema).as("data")).select("data.*");


        List<Message> messageCensor = new ArrayList<>();

        Dataset<Row> censoredTable = set.withColumn("cens_word",
                        when(col("word")
                                        .isInCollection(censoredWords),
                                wordToChange)
                                .otherwise(col("word")))
                .select("speaker", "time", "cens_word");

        logger.info("Aggregate operation");

        Dataset<Row> aggregated = censoredTable
                .withWatermark("time", "0 seconds")
                .withColumn("window", window(col("time"), windowDuration+" minutes"))
                .groupBy("speaker", "window")
                .agg(collect_set("cens_word").as("all_words"))
                .select("window", "all_words", "speaker");

        Dataset<Row> censoredCount = censoredTable
                .filter("cens_word == 'AAAAAAA'")
                .groupBy("speaker")
                .count().as("censored_count");

        StreamingQuery query = aggregated
                .writeStream()
                .outputMode("update")
                .format("console")
                .start();

        StreamingQuery query1 = censoredCount
                .writeStream()
                .outputMode("update")
                .format("console")
                .start();

        query.awaitTermination();
        query1.awaitTermination();
    }
}
