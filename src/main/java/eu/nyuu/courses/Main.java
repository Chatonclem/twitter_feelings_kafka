package eu.nyuu.courses;

import edu.stanford.nlp.simple.*;
import eu.nyuu.courses.model.SensorEvent;
import eu.nyuu.courses.model.Sentiment;
import eu.nyuu.courses.model.TweetSentiment;
import eu.nyuu.courses.serdes.SerdeFactory;
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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Main {

    public static void main(final String[] args) {

        Sentence sent = new Sentence("Lucy is in the sky with diamonds.");
        SentimentClass sentiment = sent.sentiment();
        sentiment.toString();
        final String bootstrapServers = args.length > 0 ? args[0] : "51.15.90.153:9092";
        final Properties streamsConfiguration = new Properties();

        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "twitter_app_tmp");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "twitter_app_tmp");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        final Serde<String> stringSerde = Serdes.String();
        final Map<String, Object> serdeProps = new HashMap<>();
        final Serde<SensorEvent> sensorEventSerde = SerdeFactory.createSerde(SensorEvent.class, serdeProps);
        final Serde<TweetSentiment> sensorTweetSentiment = SerdeFactory.createSerde(TweetSentiment.class, serdeProps);
        final Serde<Sentiment> sensorSentiment = SerdeFactory.createSerde(Sentiment.class, serdeProps);

        // Stream
        final StreamsBuilder builder = new StreamsBuilder();

        // Here you go :)
        builder
                .stream("tweets", Consumed.with(Serdes.String(), sensorEventSerde))
                .map((key,tweet) -> KeyValue.pair(tweet.getId(), new TweetSentiment(tweet.getNick(), tweet.getTimestamp(), tweet.getBody(), new Sentence(tweet.getBody()).sentiment().toString())))
                .to("tardicery_2", Produced.with(Serdes.String(), sensorTweetSentiment));

        KTable<String, Sentiment> user_table = builder
                .stream("tardicery_2", Consumed.with(
                        Serdes.String(),
                        sensorTweetSentiment))
                .groupBy((k, v) -> v.getUser())
                .aggregate(
                        () -> new Sentiment(0,0,0),
                        (aggKey, newValue, aggValue) -> {
                            if (newValue.getSentiment() == "POSITIVE") {
                                return new Sentiment(aggValue.getPositive() +1, aggValue.getNeutral(), aggValue.getNegative());
                            } else if (newValue.getSentiment() == "NEUTRAL") {
                                return new Sentiment(aggValue.getPositive(), aggValue.getNeutral() + 1, aggValue.getNegative());
                            } else {
                                return new Sentiment(aggValue.getPositive(), aggValue.getNeutral(), aggValue.getNegative() + 1);
                            }
                        },
                        Materialized.<String, Sentiment, KeyValueStore< Bytes, byte[]>>
                                as("tardicery_user_sentiment")
                                .withKeySerde(stringSerde).withValueSerde(sensorSentiment)
                );

        KTable<String, Sentiment>  count_sentiment = builder
                .stream("tardicery_2", Consumed.with(
                        Serdes.String(),
                        sensorTweetSentiment
                ))
                .groupBy((k,v) -> "KEY")
                .aggregate(
                        () -> new Sentiment(0,0,0),
                        (aggKey, newValue, aggValue) -> {
                            if (newValue.getSentiment() == "POSITIVE") {
                                return new Sentiment(aggValue.getPositive() +1, aggValue.getNeutral(), aggValue.getNegative());
                            } else if (newValue.getSentiment() == "NEUTRAL") {
                                return new Sentiment(aggValue.getPositive(), aggValue.getNeutral() + 1, aggValue.getNegative());
                            } else {
                                return new Sentiment(aggValue.getPositive(), aggValue.getNeutral(), aggValue.getNegative() + 1);
                            }
                        },
                        Materialized.<String, Sentiment, KeyValueStore< Bytes, byte[]>>
                                as("tardicery_global_sentiment")
                                .withKeySerde(stringSerde).withValueSerde(sensorSentiment)
                );


        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);

        streams.cleanUp();
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
};
