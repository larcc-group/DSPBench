package spark.streaming.application;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import spark.streaming.constants.SpikeDetectionConstants;
import spark.streaming.constants.WordCountConstants;
import spark.streaming.function.*;
import spark.streaming.model.Moving;
import spark.streaming.util.Configuration;
import spark.streaming.util.Tuple;

import java.util.List;

public class SpikeDetectionSS extends AbstractApplication {
    private static final Logger LOG = LoggerFactory.getLogger(SpikeDetectionSS.class);
    private int parserThreads;
    private int movingAverageThreads;
    private int spikeDetectorThreads;
    private int batchSize;
    private int maxRatePerPartition;
    private String checkpointPath;
    public SpikeDetectionSS(String appName, Configuration config) {
        super(appName, config);
    }

    @Override
    public void initialize() {
        parserThreads = config.getInt(SpikeDetectionConstants.Config.PARSER_THREADS, 1);
        movingAverageThreads = config.getInt(SpikeDetectionConstants.Config.MOVING_AVERAGE_THREADS, 1);
        spikeDetectorThreads = config.getInt(SpikeDetectionConstants.Config.SPIKE_DETECTOR_THREADS, 1);
        batchSize            = config.getInt(getConfigKey(WordCountConstants.Config.BATCH_SIZE), 1000);
        maxRatePerPartition  = config.getInt(getConfigKey(WordCountConstants.Config.maxRatePerPartition), 1000);
        checkpointPath         = config.get(getConfigKey(WordCountConstants.Config.CHECKPOINT_PATH), ".");
    }

    @Override
    public DataStreamWriter buildApplication() {
       return null;
    }

    @Override
    public JavaStreamingContext buildApplicationStreaming() {

        context = new JavaStreamingContext(config, Durations.milliseconds(batchSize));
        context.checkpoint(checkpointPath);

        JavaDStream<String> rawRecords = createSourceSS();
        JavaDStream<Tuple> records = rawRecords
                .repartition(parserThreads)
                .map(new SensorParser(config));

        JavaMapWithStateDStream<Integer, Tuple, Moving, Tuple> averages = records.filter(new FilterNull<Tuple>())
                .repartition(movingAverageThreads)
                .mapToPair(f -> new Tuple2<Integer, Tuple>(f.getInt("MOTEID"), f))
                .mapWithState(StateSpec.function(new MovingAverage(config)));

        JavaDStream<Tuple> spikes = averages
                .repartition(spikeDetectorThreads)
                .map(new SpikeDetector(config))
                .filter(new FilterNull<Tuple>());

        createSinkSS2(spikes);
        return context;
    }

    @Override
    public String getConfigPrefix() {
        return SpikeDetectionConstants.PREFIX;
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }
}
