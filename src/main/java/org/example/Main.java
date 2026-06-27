package org.example;

import org.apache.kafka.clients.producer.*;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.List;
import java.util.Properties;

public class Main {

    private static KafkaProducer<String, String> producer;

    // ✅ REQUIREMENT: Custom filtering logic — filter by keywords
    private static final List<String> KEYWORDS = List.of(
             "machine learning", "python", "tech", "coding",
            "software", "data", "cloud", "java", "open source"
    );

    private static boolean matchesFilter(String text) {
        String lower = text.toLowerCase();
        return KEYWORDS.stream().anyMatch(kw -> lower.contains(kw.toLowerCase()));
    }

    public static void main(String[] args) throws Exception {
        try {
            runWindowsCommand("docker compose down -v");
            runWindowsCommand("docker compose up -d");
        } catch (Exception e) {
            System.err.println("Failed to reset Kafka: " + e.getMessage());
            e.printStackTrace();
        }
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<>(props);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n🔴 Stopping stream...");
            producer.flush();
            producer.close();
        }));

        String url = "wss://jetstream1.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post";
        System.out.println("Connecting to Bluesky...");

        WebSocketClient client = new WebSocketClient(new URI(url)) {

            @Override
            public void onOpen(ServerHandshake handshake) {
                System.out.println("✅ Connected! Streaming with keyword filter: " + KEYWORDS);
            }

            @Override
            public void onMessage(String message) {
                try {
                    JSONObject data = new JSONObject(message);

                    if ("commit".equals(data.optString("kind"))) {
                        JSONObject commit = data.optJSONObject("commit");
                        if (commit == null) return;

                        if ("create".equals(commit.optString("operation"))) {
                            JSONObject record = commit.optJSONObject("record");
                            if (record == null) return;

                            String text = record.optString("text", "");
                            String did  = data.optString("did", "unknown");

                            if (text.isEmpty()) return;

                            // ✅ REQUIREMENT: Apply filtering BEFORE publishing to Kafka
                            if (!matchesFilter(text)) {
                                return; // drop posts that don't match any keyword
                            }

                            String cleanText = text.replace("\n", " ");

                            JSONObject post = new JSONObject();
                            post.put("user", did);
                            post.put("text", cleanText);
                            post.put("platform", "Bluesky");

                            ProducerRecord<String, String> record2 =
                                    new ProducerRecord<>("posts", post.toString());

                            producer.send(record2, (metadata, ex) -> {
                                if (ex != null) {
                                    System.err.println("❌ Delivery failed: " + ex.getMessage());
                                } else {
                                    System.out.printf("✅ Delivered to %s | partition %d | offset %d%n",
                                            metadata.topic(), metadata.partition(), metadata.offset());
                                }
                            });

                            System.out.printf("%n[MATCH][User: %s]%n%s%n", did, cleanText);
                        }
                    }

                } catch (Exception e) {
                    // skip malformed messages
                }
            }

            @Override
            public void onClose(int code, String reason, boolean remote) {
                System.out.println("🔴 Connection closed: " + reason);
                producer.flush();
            }

            @Override
            public void onError(Exception e) {
                System.err.println("❌ Error: " + e.getMessage());
                producer.flush();
            }
        };

        client.connectBlocking();
        Thread.currentThread().join();
    }

    /**
     * Executes a command via the Windows command prompt
     */
    private static void runWindowsCommand(String command) throws IOException, InterruptedException {
        ProcessBuilder processBuilder = new ProcessBuilder("cmd.exe", "/c", command);

        // Maps the command output directly to your Java console so you can see what Docker is doing
        processBuilder.inheritIO();

        Process process = processBuilder.start();
        int exitCode = process.waitFor();

        if (exitCode != 0) {
            throw new RuntimeException("Command failed with exit code: " + exitCode + ". Command was: " + command);
        }
    }
}