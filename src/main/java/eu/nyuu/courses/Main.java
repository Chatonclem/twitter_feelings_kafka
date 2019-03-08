package eu.nyuu.courses;

import edu.stanford.nlp.simple.*;
import eu.nyuu.courses.model.SensorEvent;
import eu.nyuu.courses.model.Sentiment;
import eu.nyuu.courses.model.TweetSentiment;
import eu.nyuu.courses.serdes.SerdeFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.*;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {

    public static void main(final String[] args) {

        final String bootstrapServers = args.length > 0 ? args[0] : "51.15.90.153:9092";
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "tardicery3_twitter_app");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "tardicery3_twitter_app");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "C:\\Users\\Adrian\\Projects\\tmp");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");


        final Serde<String> stringSerde = Serdes.String();
        final Map<String, Object> serdeProps = new HashMap<>();
        final Serde<SensorEvent> sensorEventSerde = SerdeFactory.createSerde(SensorEvent.class, serdeProps);
        final Serde<TweetSentiment> sensorTweetSentiment = SerdeFactory.createSerde(TweetSentiment.class, serdeProps);
        final Serde<Sentiment> sensorSentiment = SerdeFactory.createSerde(Sentiment.class, serdeProps);
        // Stream
        final StreamsBuilder builder = new StreamsBuilder();

        // Here you go :)
        KStream<String, TweetSentiment> stream_principal = builder
                .stream("tweets", Consumed.with(Serdes.String(), sensorEventSerde))
                .map((key, tweet) -> KeyValue.pair(tweet.getId(), new TweetSentiment(tweet.getNick(), tweet.getTimestamp(), tweet.getBody(), new Sentence(tweet.getBody()).sentiment().toString())));

        KTable<Windowed<String>, Sentiment> user_table = stream_principal
                .groupBy((k, v) -> v.getUser(), Grouped.with(stringSerde, sensorTweetSentiment))
                .windowedBy(TimeWindows.of(Duration.ofMinutes(5)))
                .aggregate(
                        () -> new Sentiment(0,0,0),
                        (aggKey, newValue, aggValue) -> {
                            if (newValue.getSentiment().equals("POSITIVE") || newValue.getSentiment().equals("VERY POSITIVE")) {
                                return new Sentiment(aggValue.getPositive() +1, aggValue.getNeutral(), aggValue.getNegative());
                            } else if (newValue.getSentiment().equals("NEUTRAL")) {
                                return new Sentiment(aggValue.getPositive(), aggValue.getNeutral() + 1, aggValue.getNegative());
                            } else {
                                return new Sentiment(aggValue.getPositive(), aggValue.getNeutral(), aggValue.getNegative() + 1);
                            }
                        },
                        Materialized.<String, Sentiment, WindowStore<Bytes, byte[]>>
                                as("app2_tardicery_user_sentiment")
                                .withKeySerde(stringSerde).withValueSerde(sensorSentiment)
                );

        KTable< Windowed<String>, Sentiment>  count_sentiment = stream_principal
                .groupBy((k,v) -> "KEY", Grouped.with(stringSerde, sensorTweetSentiment))
                .windowedBy(TimeWindows.of(Duration.ofMinutes(5)))
                .aggregate(
                        () -> new Sentiment(0,0,0),
                        (aggKey, newValue, aggValue) -> {
                            if (newValue.getSentiment().equals("POSITIVE") || newValue.getSentiment().equals("VERY POSITIVE")) {
                                return new Sentiment(aggValue.getPositive() +1, aggValue.getNeutral(), aggValue.getNegative());
                            } else if (newValue.getSentiment().equals("NEUTRAL")) {
                                return new Sentiment(aggValue.getPositive(), aggValue.getNeutral() + 1, aggValue.getNegative());
                            } else {
                                return new Sentiment(aggValue.getPositive(), aggValue.getNeutral(), aggValue.getNegative() + 1);
                            }
                        },
                        Materialized.<String, Sentiment, WindowStore<Bytes, byte[]>>
                                as("app2_tardicery_global_sentiment")
                                .withKeySerde(stringSerde).withValueSerde(sensorSentiment)
                );


        // Hashtags handling
/*
        final Pattern p = Pattern.compile("\\B(\\#[a-zA-Z]+\\b)(?!;)");

        KTable<Windowed<String>, Integer> count_pop_hashtags = stream_principal
                .groupBy((k,v) -> {
                    Matcher m = p.matcher(v.getBody());
                    if (m.find()) {

                        m.group(0);
                    }
                }, Grouped.with(stringSerde, Integer))
                .
*/
        count_sentiment.toStream().print(Printed.toSysOut());


        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);

        streams.cleanUp();
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        //Interactive Query

        try {
            Thread.sleep(Duration.ofMinutes(1).toMillis());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        while (true) {
            if (streams.state() == KafkaStreams.State.RUNNING) {
                // Querying our local store
                ReadOnlyWindowStore<String, Sentiment> windowStore =
                        streams.store("app2_tardicery_global_sentiment", QueryableStoreTypes.windowStore());

                // fetching all values for the last minute in the window
                Instant now = Instant.now();
                Instant lastMinute = now.minus(Duration.ofHours(1));

                KeyValueIterator<Windowed<String>, Sentiment> iterator = windowStore.fetchAll(lastMinute, now);
                while (iterator.hasNext()) {
                    KeyValue<Windowed<String>, Sentiment> next = iterator.next();
                    Windowed<String> windowTimestamp = next.key;
                    System.out.println("Count of Positive " + windowTimestamp.key() + "@ " + windowTimestamp + " is " + next.value.getPositive());
                    System.out.println("Count of Neutral " + windowTimestamp.key() + "@ " + windowTimestamp + " is " + next.value.getNeutral());
                    System.out.println("Count of Negative " + windowTimestamp.key() + "@ " + windowTimestamp + " is " + next.value.getNegative());
                }
                // close the iterator to release resources
                iterator.close();
            }

            // Dumping all keys every minute
            try {
                Thread.sleep(Duration.ofMinutes(1).toMillis());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
};
