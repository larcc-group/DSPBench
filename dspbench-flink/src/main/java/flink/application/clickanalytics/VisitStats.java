package flink.application.clickanalytics;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class VisitStats implements FlatMapFunction<Tuple4<String, String, String, String>, Tuple3<Integer, Integer, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(VisitStats.class);

    private static int total = 0;
    private static int uniqueCount = 0;

    public VisitStats(Configuration config) {

    }

    @Override
    public void flatMap(Tuple4<String, String, String, String> input, Collector<Tuple3<Integer, Integer, String>> out) {
        boolean unique = Boolean.parseBoolean(input.getField(2));
        total++;
        if(unique) uniqueCount++;
        out.collect( new Tuple3<Integer, Integer, String>(total, uniqueCount, input.f3));
        //super.calculateThroughput();
    }
}