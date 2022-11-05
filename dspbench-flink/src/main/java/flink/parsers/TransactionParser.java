package flink.parsers;

import com.google.common.collect.ImmutableList;
import flink.util.Metrics;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

import java.time.Instant;

/**
 *
 */
public class TransactionParser extends Metrics implements MapFunction<String, Tuple3<String, String, String>> {

    @Override
    public Tuple3<String, String, String> map(String value) throws Exception {
        super.calculateThroughput();
        String[] temp = value.split(",", 2);
        return new Tuple3<>(
                temp[0],
                temp[1],
                Instant.now().toEpochMilli() + ""
        );
    }
    
}