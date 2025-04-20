package flink.parsers;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import flink.application.highprocessingtimevariance.HighProcessingTimeVarianceEvent;
import flink.util.Configurations;
import flink.util.Metrics;

import java.util.List;
import java.util.Arrays;
import java.util.stream.Collectors;

public class HighProcessingTimeVarianceParser implements FlatMapFunction<String, HighProcessingTimeVarianceEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(HighProcessingTimeVarianceParser.class);

    Configuration config;
    String sourceName;

    Metrics metrics = new Metrics();

    public HighProcessingTimeVarianceParser(Configuration config, String sourceName){
        metrics.initialize(config, this.getClass().getSimpleName()+"-"+sourceName);
        this.config = config;
        this.sourceName = sourceName;
    }

    public void flatMap(String value, Collector<HighProcessingTimeVarianceEvent> out) throws Exception {
        metrics.initialize(config, this.getClass().getSimpleName()+"-"+sourceName);

        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.receiveThroughput();
        }

        String[] record = value.split(",");
        int id = Integer.parseInt(record[0]);
        List<Integer> durations = Arrays.stream(record[1].split(" "))
                                                        .map(Integer::parseInt)
                                                        .collect(Collectors.toList());

        HighProcessingTimeVarianceEvent event = new HighProcessingTimeVarianceEvent(id, durations);

        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.emittedThroughput();
        }

        out.collect(event);
    }       
}
