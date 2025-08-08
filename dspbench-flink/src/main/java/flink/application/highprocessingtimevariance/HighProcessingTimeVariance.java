package flink.application.highprocessingtimevariance;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import flink.application.AbstractApplication;
import flink.constants.HighProcessingTimeVarianceConstants;
import flink.parsers.HighProcessingTimeVarianceParser;


public class HighProcessingTimeVariance extends AbstractApplication {

    public class MetadataGenerator implements FlatMapFunction<HighProcessingTimeVarianceEvent, HighProcessingTimeVarianceMetadata> {

        @Override
        public void flatMap(HighProcessingTimeVarianceEvent event, Collector<HighProcessingTimeVarianceMetadata> out) throws Exception {
            List<Integer> durations = event.getDurations();  
            for (int i = 0; i < durations.size(); i++) {
                int duration = durations.get(i);
                HighProcessingTimeVarianceMetadata metadata = new HighProcessingTimeVarianceMetadata(event.getId(), duration);
                out.collect(metadata);
            }
        }
    }

    public class MetadataCollector implements MapFunction<HighProcessingTimeVarianceMetadata, HighProcessingTimeVarianceEvent> {

        @Override
        public HighProcessingTimeVarianceEvent map(HighProcessingTimeVarianceMetadata metadata) throws Exception {
            int delay = metadata.getDuration();
            Thread.sleep(delay);
             HighProcessingTimeVarianceEvent wrappedEvent = new HighProcessingTimeVarianceEvent(
                    metadata.getEventId()
            );
            wrappedEvent.getMetadatas().add(metadata);
            return wrappedEvent;
        }
    }

    public class MetadataReducer implements ReduceFunction<HighProcessingTimeVarianceEvent> {

        @Override
        public HighProcessingTimeVarianceEvent reduce(HighProcessingTimeVarianceEvent event1, HighProcessingTimeVarianceEvent event2) throws Exception {
            event1.getMetadatas().addAll(event2.getMetadatas());
            return event1;
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(HighProcessingTimeVariance.class);

    private int parserThreads;
    private int collectorThreads;
    private int reducerThreads;

    public HighProcessingTimeVariance(String appName, Configuration config) {
        super(appName, config);
    }

    @Override
    public void initialize() {
        this.parserThreads = config.getInteger(HighProcessingTimeVarianceConstants.Conf.PARSER_THREADS, 1);
        this.collectorThreads= config.getInteger(HighProcessingTimeVarianceConstants.Conf.COLLECTOR_THREADS, 1);
        this.reducerThreads = config.getInteger(HighProcessingTimeVarianceConstants.Conf.REDUCER_THREADS, 1);
    }

    @Override
    public StreamExecutionEnvironment buildApplication() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Spout
        DataStream<String> spout = createSource("highprocessingtimevariance");

        // Parser
        DataStream<HighProcessingTimeVarianceEvent> parser = spout
            .flatMap(new HighProcessingTimeVarianceParser(this.config, "highprocessingtimevariance"))
            .name("Parser");

        // (1) Generate metadata
        DataStream<HighProcessingTimeVarianceMetadata> generated = parser
            .flatMap(new MetadataGenerator())
            .name("MetadataGenerator")
            .setParallelism(this.parserThreads);

        // (2) Collect metadata (simulated delay)
        DataStream<HighProcessingTimeVarianceEvent> collected = generated
            .map(new MetadataCollector())
            .name("MetadataCollector")
            .setParallelism(this.collectorThreads)
            .startNewChain();

        // (3) Keyed reduce
        DataStream<HighProcessingTimeVarianceEvent> reduced = collected
            .keyBy(HighProcessingTimeVarianceEvent::getId)
            .reduce(new MetadataReducer())
            .name("EventReducer")
            .setParallelism(this.reducerThreads);

        // Sink
        createSinkHPTV(reduced);

        return env;
    }

    @Override
    public String getConfigPrefix() {
        return HighProcessingTimeVarianceConstants.PREFIX;
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }   
}
