# 🌊 Real-Time Social Media Stream Processor

A real-time data pipeline that streams posts from **Bluesky** (a decentralized social network), filters them by keyword, ingests them into **Apache Kafka**, and processes them with **Apache Spark Structured Streaming** to perform live word count aggregation.

> Built as a Big Data Engineering project — demonstrating end-to-end stream processing from a live social media source to real-time analytics.

---

## 📐 Architecture

```
Bluesky Jetstream (WebSocket)
        │
        ▼
 ┌─────────────────┐
 │  Kafka Producer  │  ← Keyword Filter (Java)
 │   (Main.java)   │
 └────────┬────────┘
          │
          ▼  topic: "posts"
 ┌─────────────────┐
 │  Apache Kafka   │  ← Docker Compose
 └────────┬────────┘
          │
          ▼
 ┌────────────────────────┐
 │  Spark Structured      │  ← JSON Parsing + Word Count
 │  Streaming             │     (SparkWordCount.java)
 │  (SparkWordCount.java) │
 └────────────────────────┘
          │
          ▼
    Console Output
    (Top 30 words, refreshed per micro-batch)
```

---

## ✨ Features

- **Live WebSocket ingestion** from Bluesky's Jetstream API (`wss://jetstream1.us-east.bsky.network`)
- **Keyword filtering** before publishing to Kafka — only tech-related posts are ingested (e.g. `machine learning`, `python`, `cloud`, `java`, `open source`)
- **Kafka producer** with async delivery callbacks and graceful shutdown hook
- **Spark Structured Streaming** with:
  - JSON field extraction using `get_json_object` (avoids polluting word counts with JSON keys)
  - Tokenization, punctuation removal, and noise filtering
  - Running word count aggregation in `complete` output mode
  - Top 30 most frequent words displayed per micro-batch

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|-----------|
| Data Source | Bluesky Jetstream WebSocket API |
| Message Broker | Apache Kafka (via Docker) |
| Stream Processing | Apache Spark Structured Streaming |
| Language | Java 11+ |
| Build Tool | Apache Maven |
| Containerization | Docker + Docker Compose |

---

## 📁 Project Structure

```
bluesky-spark/
├── src/
│   └── main/
│       └── java/
│           └── org/example/
│               ├── Main.java              # Kafka producer + Bluesky WebSocket client
│               └── SparkWordCount.java    # Spark Structured Streaming consumer
├── docker-compose.yml                     # Kafka + Zookeeper setup
├── pom.xml                                # Maven dependencies
├── target/
│   ├── bluesky-spark-1.0-SNAPSHOT.jar
│   └── bluesky-spark-1.0-SNAPSHOT-jar-with-dependencies.jar
└── README.md
```

---

## ⚙️ Prerequisites

Make sure you have the following installed:

- **Java 11+** — [Download](https://adoptium.net/)
- **Apache Maven 3.6+** — [Download](https://maven.apache.org/)
- **Apache Spark 3.x** — [Download](https://spark.apache.org/downloads.html)
- **Docker Desktop** — [Download](https://www.docker.com/products/docker-desktop/)

---

## 🚀 Getting Started

### 1. Clone the repository

```bash
git clone https://github.com/<your-username>/bluesky-spark.git
cd bluesky-spark
```

### 2. Build the project

```bash
mvn clean package -DskipTests
```

This produces two JARs in `target/`:
- `bluesky-spark-1.0-SNAPSHOT.jar` — thin JAR (requires dependencies on classpath)
- `bluesky-spark-1.0-SNAPSHOT-jar-with-dependencies.jar` — fat JAR (self-contained)

### 3. Start Kafka with Docker

The Kafka producer (`Main.java`) automatically runs `docker compose up -d` on startup. To start it manually:

```bash
docker compose up -d
```

To reset Kafka (wipe topics and offsets):

```bash
docker compose down -v
docker compose up -d
```

### 4. Run the Kafka Producer (Bluesky Ingestion)

```bash
java -cp target/bluesky-spark-1.0-SNAPSHOT-jar-with-dependencies.jar org.example.Main
```

You should see:

```
✅ Connected! Streaming with keyword filter: [machine learning, python, tech, ...]
[MATCH][User: did:plc:xyz...]
✅ Delivered to posts | partition 0 | offset 42
```

### 5. Run the Spark Word Count Consumer

In a **separate terminal**, submit the Spark job:

```bash
spark-submit \
  --class org.example.SparkWordCount \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  target/bluesky-spark-1.0-SNAPSHOT-jar-with-dependencies.jar
```

> ⚠️ Make sure the Kafka version in `--packages` matches your Spark version.

---

## 📊 Sample Output

```
-------------------------------------------
Batch: 3
-------------------------------------------
+----------+-----+
|word      |count|
+----------+-----+
|python    |87   |
|data      |74   |
|learning  |61   |
|machine   |58   |
|open      |43   |
|source    |41   |
|cloud     |39   |
|java      |35   |
|software  |28   |
|tech      |25   |
+----------+-----+
```

---

## 🔧 Configuration

### Keyword Filter

Edit the `KEYWORDS` list in `Main.java` to control which posts get ingested:

```java
private static final List<String> KEYWORDS = List.of(
    "machine learning", "python", "tech", "coding",
    "software", "data", "cloud", "java", "open source"
);
```

### Kafka Settings

Kafka defaults to `localhost:9092`. Update in both files if your broker is on a different host/port:

- `Main.java` → `props.put("bootstrap.servers", "localhost:9092")`
- `SparkWordCount.java` → `.option("kafka.bootstrap.servers", "localhost:9092")`

### Spark Checkpoint

Word count state is checkpointed to `./checkpoint_wordcount`. Delete this directory to reset aggregation state:

```bash
rm -rf ./checkpoint_wordcount
```

---

## 🪟 Windows Notes

The producer uses `cmd.exe` to manage Docker Compose:

```java
new ProcessBuilder("cmd.exe", "/c", command)
```

If you're on **Linux/macOS**, replace the `runWindowsCommand` method with:

```java
private static void runCommand(String command) throws IOException, InterruptedException {
    ProcessBuilder pb = new ProcessBuilder("bash", "-c", command);
    pb.inheritIO();
    int code = pb.start().waitFor();
    if (code != 0) throw new RuntimeException("Command failed: " + command);
}
```

---

## 🤝 Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you'd like to change.

---

## 📄 License

This project is licensed under the [MIT License](LICENSE).

---

## 👤 Author

**Mostafa** — Software Engineering Student, Helwan University (FCAIH)

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-blue?logo=linkedin)](https://linkedin.com/in/<your-linkedin>)
[![GitHub](https://img.shields.io/badge/GitHub-Follow-black?logo=github)](https://github.com/<your-username>)
