package spark.streaming.application;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import spark.streaming.constants.WordCountConstants;
import spark.streaming.function.SSFilterNull;
import spark.streaming.function.SSWordCount;
import spark.streaming.function.SSWordcountParser;
import spark.streaming.function.Split;
import spark.streaming.util.Configuration;
import spark.streaming.util.Tuple;

import java.util.Arrays;

public class WordCountSS extends AbstractApplication {
    private static final Logger LOG = LoggerFactory.getLogger(WordCountSS.class);

    private int parserThreads;
    private int splitterThreads;
    private int singleCounterThreads;
    private int pairCounterThreads;
    private int batchSize;

    public WordCountSS(String appName, Configuration config) {
        super(appName, config);
    }

    @Override
    public void initialize() {
        splitterThreads = config.getInt(WordCountConstants.Config.SPLITTER_THREADS, 1);
        singleCounterThreads = config.getInt(WordCountConstants.Config.SINGLE_COUNTER_THREADS, 1);
        pairCounterThreads = config.getInt(WordCountConstants.Config.PAIR_COUNTER_THREADS, 1);
        parserThreads = config.getInt(WordCountConstants.Config.PARSER_THREADS, 1);
        batchSize            = config.getInt(getConfigKey(WordCountConstants.Config.BATCH_SIZE), 1000);
    }

    @Override
    public DataStreamWriter buildApplication() throws StreamingQueryException {
        return null;
    }

    @Override
    public JavaStreamingContext buildApplicationStreaming() {

        config.set("backpressure.enabled", "true");//todo add to config file
        config.set("kafka.maxRatePerPartition", "1000");//todo add to config file
        context = new JavaStreamingContext(config, Durations.milliseconds(batchSize)); //todo change for conf file

        JavaDStream<String> lines  = createSourceSS();

        JavaDStream<String> words = lines.repartition(splitterThreads).flatMap(x -> Arrays.asList(x.split(" ")).iterator());

        JavaPairDStream<String, Integer> pairs = words.repartition(singleCounterThreads).mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairDStream<String, Integer> wordCounts = pairs.repartition(pairCounterThreads).reduceByKey((i1, i2) -> i1 + i2);

        createSinkSS(wordCounts);

        return context;
    }

    @Override
    public String getConfigPrefix() {
        return WordCountConstants.PREFIX;
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }
}
