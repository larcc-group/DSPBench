package flink.application.spikedetection;

import flink.application.AbstractApplication;
import flink.application.sentimentanalysis.SentimentCalculator;
import flink.constants.SentimentAnalysisConstants;
import flink.constants.SpikeDetectionConstants;
import flink.parsers.JsonTweetParser;
import flink.parsers.SensorParser;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class SpikeDetection extends AbstractApplication {

    private static final Logger LOG = LoggerFactory.getLogger(SpikeDetection.class);
    private int movingAverageThreads;
    private int spikeDetectorThreads;

    public SpikeDetection(String appName, Configuration config) {
        super(appName, config);
    }

    @Override
    public void initialize() {
        movingAverageThreads = config.getInteger(SpikeDetectionConstants.Conf.MOVING_AVERAGE_THREADS, 1);
        spikeDetectorThreads = config.getInteger(SpikeDetectionConstants.Conf.SPIKE_DETECTOR_THREADS, 1);
    }

    @Override
    public StreamExecutionEnvironment buildApplication() {

        env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Spout
        DataStream<String> data = createSource();

        // Parser
        DataStream<Tuple4<String, Date, Double, String>> dataParse = data.map(new SensorParser(config));

        // Process
        DataStream<Tuple4<String, Double, Double, String>> movingAvg = dataParse.filter(value -> (value != null)).keyBy(value -> value.f0).flatMap(new MovingAverage(config)).setParallelism(movingAverageThreads);

        DataStream<Tuple5<String, Double, Double, String, String>> spikeDetect = movingAvg.filter(value -> (value != null)).flatMap(new SpikeDetect(config)).setParallelism(spikeDetectorThreads);
        // Sink
        createSink(spikeDetect);

        return env;
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