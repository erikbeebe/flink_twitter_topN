# Flink Twitter TopN
Find top N hashtags associated with tweets containing specific terms

# How do I configure things?
You can just the value for topN and the list of hashtags to extract by modifying:
```java
public static final Integer HASHTAG_LIMIT = 20;
public static final List<String> TagArray = new ArrayList<String>(Arrays.asList("NASA", "Discovery", "Interstellar"));
```

# How to run?

## Local
First, you can execute this locally, without using a Flink cluster, by simply
editing pom.xml, and update the commandlineArgs section to contain your valid
Twitter API keys, and then build and run:

```bash
$ mvn clean package
$ mvn exec:java
```

## Cluster
If you're running a Flink cluster (and have the 'flink' command in your
path), you can simply edit the start-twitter.sh script and fill in the
values for:

```bash
flink run -p 1 -c io.eventador.flinktwitter.FlinkTwitter ./target/flink-twitter-1.0-SNAPSHOT.jar \
    --consumer_key "CONSUMER_KEY" \
    --consumer_secret "CONSUMER_SECRET" \
    --token "TOKEN" \
    --token_secret "TOKEN_SECRET" \
    --topic "hashtags" \
    --bootstrap.servers "localhost:9092"
```

(You can get new Twitter API keys by visiting https://apps.twitter.com/
and creating a new test application.)

Once you've done that, simply build the JAR with

```bash
$ mvn clean package
```

and then submit it to the cluster by using the script.  If it's working, you'll see output
both in stdout, and if you have a Kafka endpoint, in the Kafka topic that you've selected.
