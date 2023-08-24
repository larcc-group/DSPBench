package spark.streaming.source;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import spark.streaming.constants.BaseConstants.BaseConfig;
import spark.streaming.util.Configuration;

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
    public JavaDStream<String> createStream() {
        return null;//context.receiverStream().
    }
    
}
