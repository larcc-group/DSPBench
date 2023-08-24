package spark.streaming.source;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import spark.streaming.util.Configuration;

public abstract class BaseSourceSS {

    protected Configuration config;
    protected JavaStreamingContext context;

    public void initialize(Configuration config, JavaStreamingContext context, String prefix) {
        this.config = config;
        this.context = context;
    }
    public abstract JavaDStream<String> createStream();
}

