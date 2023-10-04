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
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import spark.streaming.constants.WordCountConstants;
import spark.streaming.function.*;
import spark.streaming.model.Moving;
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
    private String checkpointPath;
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
        checkpointPath         = config.get(getConfigKey(WordCountConstants.Config.CHECKPOINT_PATH), ".");
    }

    @Override
    public DataStreamWriter buildApplication() throws StreamingQueryException {
        return null;
    }

    @Override
    public JavaStreamingContext buildApplicationStreaming() {

     //   config.set("backpressure.enabled", "true");//todo add to config file
    //    config.set("kafka.maxRatePerPartition", "1000000");//todo add to config file
        context = new JavaStreamingContext(config, Durations.milliseconds(batchSize)); //todo change for conf file
        context.checkpoint(checkpointPath);
        JavaDStream<String> lines  = createSourceSS();

        JavaDStream<String> words = lines.repartition(splitterThreads)
                .flatMap(new WordcountParser(config));
               // .flatMap(x -> Arrays.asList(x.split(" ")).iterator());

     ///   JavaPairDStream<String, Integer> pairs = words.repartition(singleCounterThreads).mapToPair(s -> new Tuple2<>(s, 1));

        //JavaPairDStream<String, Integer> wordCounts = pairs.repartition(pairCounterThreads).reduceByKey((i1, i2) -> i1 + i2);

        JavaMapWithStateDStream<String, Integer, Integer, Tuple> wordCounts = words//.filter(new FilterNull<Tuple>())
                .repartition(pairCounterThreads)
                .mapToPair(f -> new Tuple2<String, Integer>(f, 1))
                .mapWithState(StateSpec.function(new CountWordPairs(config)));

        createSinkSS2(wordCounts);

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
