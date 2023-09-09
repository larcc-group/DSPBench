package spark.streaming.sink;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import spark.streaming.metrics.MetricsFactory;
import spark.streaming.util.Configuration;
import spark.streaming.util.Tuple;

import java.io.Serializable;

public abstract class BaseSinkSS implements Serializable {
    protected Configuration config;
    protected transient SparkSession session;
    private String sinkName;
    private static MetricRegistry metrics;
    private  Counter tuplesReceived;
    private Counter tuplesEmitted;

    public void initialize(Configuration config, SparkSession session) {
        initialize(config, session, "sink");
    }
    public void initialize(Configuration config, SparkSession session, String sinkName) {
        this.config = config;
        this.session = session;
        this.sinkName = sinkName;
    }

    public abstract void sinkStream(JavaPairDStream<String, Integer> dt); //Configuration conf
    public abstract void sinkStream2(JavaDStream<Tuple> dt);
    protected MetricRegistry getMetrics() {
        if (metrics == null) {
            metrics = MetricsFactory.createRegistry(this.config);
        }
        return metrics;
    }

    protected Counter getTuplesReceived() {
        if (tuplesReceived == null) {
            tuplesReceived = getMetrics().counter(sinkName + "-received");
        }
        return tuplesReceived;
    }

    protected Counter getTuplesEmitted() {
        if (tuplesEmitted == null) {
            tuplesEmitted = getMetrics().counter(sinkName+ "-emitted");
        }
        return tuplesEmitted;
    }

    protected void incReceived() {
        getTuplesReceived().inc();
    }

    protected void incReceived(Configuration config) {
        this.config = config;
        getTuplesReceived().inc();
    }

    protected void incReceived(long n) {
        getTuplesReceived().inc(n);
    }

    protected void incEmitted() {
        getTuplesEmitted().inc();
    }

    protected void incEmitted(long n) {
        getTuplesEmitted().inc(n);
    }

    protected void incBoth() {
        getTuplesReceived().inc();
        getTuplesEmitted().inc();
    }
    public String getName() {
        return sinkName;
    }
}
