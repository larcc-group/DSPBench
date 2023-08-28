package spark.streaming.source;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import spark.streaming.constants.BaseConstants.BaseConfig;
import spark.streaming.util.Configuration;

import java.util.*;

/**
 *
 * @author mayconbordin
 */
public class KafkaSourceSS extends BaseSourceSS {
    private String kafkaTopics;
    private String kafkaHost;
    private int sourceThreads;
    private int batchSize;
    @Override
    public void initialize(Configuration config, JavaStreamingContext context, String prefix) {
        super.initialize(config, context, prefix);

        kafkaHost = config.get(String.format(BaseConfig.KAFKA_HOST, prefix));
        kafkaTopics = config.get(String.format(BaseConfig.KAFKA_SOURCE_TOPIC, prefix));
        sourceThreads = config.getInt(String.format(BaseConfig.SOURCE_THREADS, prefix), 1);
        batchSize = config.getInt(String.format(BaseConfig.BATCH_SIZE, prefix), 1000);
    }
    @Override
    public JavaDStream<String> createStream( JavaStreamingContext streamingContext) {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", kafkaHost);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
       // kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Collections.singletonList(kafkaTopics);

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );

        return stream.map(record ->  record.value());
    }
    
}
