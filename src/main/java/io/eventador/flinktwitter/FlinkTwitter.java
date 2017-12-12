package io.eventador.flinktwitter;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;

import org.apache.flink.configuration.Configuration;

import org.apache.flink.runtime.executiongraph.restart.FixedDelayRestartStrategy;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategyFactory;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.lang.String;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.flink.streaming.connectors.twitter.TwitterSource;

import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.DefaultStreamingEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;


public class FlinkTwitter {
    public static final Integer HASHTAG_LIMIT = 20;
    public static final List<String> TagArray = new ArrayList<String>(Arrays.asList("NASA", "Discovery", "Interstellar"));

    public static void main(String[] args) throws Exception {
        //ParameterTool params = ParameterTool.fromPropertiesFile(args[0]);

        // create streaming environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // enable event time processing
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.getConfig().setAutoWatermarkInterval(1000L);
        env.setParallelism(1);

        // enable fault-tolerance, 60s checkpointing
        env.enableCheckpointing(60000);

        // enable restarts
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(0, 500L));

        // Get parameters from command line
        final ParameterTool params = ParameterTool.fromArgs(args);

        if (params.getNumberOfParameters() < 6) {
            System.out.println("\nUsage: FlinkTwitter --consumer_key <consumer_key> --consumer_secret <consumer_secret> " +
					"--token <token> --token_secret <token_secret> " +
                    "--bootstrap.servers <kafka brokers> --topic <topic>");
            return;
        }

        Properties props = new Properties();
        props.setProperty(TwitterSource.CONSUMER_KEY, params.getRequired("consumer_key"));
        props.setProperty(TwitterSource.CONSUMER_SECRET, params.getRequired("consumer_secret"));
        props.setProperty(TwitterSource.TOKEN, params.getRequired("token"));
        props.setProperty(TwitterSource.TOKEN_SECRET, params.getRequired("token_secret"));

        // Configure Twitter source
        TwitterSource twitterSource = new TwitterSource(props);
        TweetFilter customFilterInitializer = new TweetFilter();
        twitterSource.setCustomEndpointInitializer(customFilterInitializer);
        DataStream<String> streamSource = env.addSource(twitterSource);

        // Parse JSON tweets, flatmap and emit keyed stream
        DataStream<Tuple2<String, Integer>> jsonTweets = streamSource.flatMap(new TweetFlatMapper())
                                                                     .keyBy(0);

        // Ordered topN list of most popular hashtags
        DataStream<LinkedHashMap<String, Integer>> ds = jsonTweets.timeWindowAll(Time.seconds(300), Time.seconds(5))
                                                            	  .apply(new MostPopularTags());
 	// Print to stdout
	ds.print();

	// Write to Kafka
        ds.addSink(new FlinkKafkaProducer010<>(
                                params.getRequired("topic"),
                                new SerializationSchema<LinkedHashMap<String, Integer>>() {
                                        @Override
                                        public byte[] serialize(LinkedHashMap<String, Integer> element) {
                                                return element.toString().getBytes();
                                        }
                                },
                                params.getProperties())
			).name("Kafka Sink");

        String app_name = String.format("Streaming Tweets");
        env.execute(app_name);
    }

    public static class TweetFilter implements TwitterSource.EndpointInitializer, Serializable {
        @Override
        public StreamingEndpoint createEndpoint() {
            StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
            endpoint.trackTerms(TagArray);
            return endpoint;
        }
    }

    private static class TweetFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String tweet, Collector<Tuple2<String, Integer>> out) throws Exception {
            ObjectMapper mapper = new ObjectMapper();
            String tweetString = null;

            Pattern p = Pattern.compile("#\\w+");

            try {
                JsonNode jsonNode = mapper.readValue(tweet, JsonNode.class);
                tweetString = jsonNode.get("text").textValue();
            } catch (Exception e) {
                // That's ok
            }

            if (tweetString != null) {
                List<String> tags = new ArrayList<>();
                Matcher matcher = p.matcher(tweetString);

                while (matcher.find()) {
                    String cleanedHashtag = matcher.group(0).trim();
					if (cleanedHashtag != null) {
                        out.collect(new Tuple2<>(cleanedHashtag, 1));
					}
                }
            }
        }
    }

    // Window functions
    public static class MostPopularTags implements AllWindowFunction<Tuple2<String, Integer>, LinkedHashMap<String, Integer>, TimeWindow> {
        @Override
        public void apply(TimeWindow window, Iterable<Tuple2<String, Integer>> tweets, Collector<LinkedHashMap<String, Integer>> collector) throws Exception {
            HashMap<String, Integer> hmap = new HashMap<String, Integer>();

            for (Tuple2<String, Integer> t: tweets) {
                int count = 0;
                if (hmap.containsKey(t.f0)) {
                    count = hmap.get(t.f0);
                }
                hmap.put(t.f0, count + t.f1);
            }

			Comparator<String> comparator = new ValueComparator(hmap);
			TreeMap<String, Integer> sortedMap = new TreeMap<String, Integer>(comparator);

            sortedMap.putAll(hmap);

			LinkedHashMap<String, Integer> sortedTopN = sortedMap
				.entrySet()
				.stream()
				.limit(HASHTAG_LIMIT)
				.collect(LinkedHashMap::new, (m, e) -> m.put(e.getKey(), e.getValue()), Map::putAll);

            collector.collect(sortedTopN);
        }
    }

    public static class ValueComparator implements Comparator<String> {
        HashMap<String, Integer> map = new HashMap<String, Integer>();
 
        public ValueComparator(HashMap<String, Integer> map){
                this.map.putAll(map);
        }
 
        @Override
        public int compare(String s1, String s2) {
                if (map.get(s1) >= map.get(s2)) {
                        return -1;
                } else {
                        return 1;
                }
        }
    }
}
