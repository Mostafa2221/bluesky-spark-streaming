package org.example;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import static org.apache.spark.sql.functions.*;

public class SparkWordCount {

    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSession.builder()
                .appName("KafkaStructuredWordCount")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        // 1. Read stream from Kafka
        Dataset<Row> kafkaDf = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "posts")
                .option("startingOffsets", "latest")
                .load();

        // 2. Convert Kafka binary value → string (raw JSON)
        Dataset<Row> messages = kafkaDf.selectExpr("CAST(value AS STRING) as json_str");

        // ✅ FIX: Parse JSON and extract only the "text" field.
        //    Without this, words like '{"user":' and '"platform":' pollute the word count.
        Dataset<Row> textOnly = messages.select(
                get_json_object(col("json_str"), "$.text").alias("text")
        ).filter(col("text").isNotNull());

        // 3. MAP: split the post text into individual words
        Dataset<Row> words = textOnly.select(
                explode(
                        split(col("text"), "\\s+")   // split on any whitespace
                ).alias("word")
        );

        // 4. CLEAN: remove empty tokens and punctuation-only tokens
        Dataset<Row> wordsClean = words
                .select(regexp_replace(col("word"), "[^a-zA-Z0-9]", "").alias("word"))
                .filter(col("word").notEqual(""))
                .filter(length(col("word")).gt(1));   // drop single-char noise

        // 5. REDUCE: word count aggregation
        Dataset<Row> wordCounts = wordsClean
                .groupBy("word")
                .count()
                .orderBy(col("count").desc());        // most frequent words first

        // 6. Output to console
        StreamingQuery query = wordCounts.writeStream()
                .format("console")
                .outputMode("complete")
                .option("truncate", "false")          // show full words, not truncated
                .option("numRows", "30")              // show top 30 words per batch
                .option("checkpointLocation", "./checkpoint_wordcount")
                .start();

        query.awaitTermination();
    }
}