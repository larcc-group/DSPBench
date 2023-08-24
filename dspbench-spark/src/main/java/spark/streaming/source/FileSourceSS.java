package spark.streaming.source;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import spark.streaming.constants.BaseConstants.BaseConfig;
import spark.streaming.util.Configuration;

/**
 * @author mayconbordin
 */
public class FileSourceSS extends BaseSourceSS {
    private int sourceThreads;
    private String sourcePath;

    @Override
    public void initialize(Configuration config, JavaStreamingContext context, String prefix) {
        super.initialize(config, context, prefix);

        sourceThreads = config.getInt(String.format(BaseConfig.SOURCE_THREADS, prefix), 1);
        sourcePath    = config.get(String.format(BaseConfig.SOURCE_PATH, prefix));
    }

    @Override
    public JavaDStream<String> createStream() {
        return context.textFileStream(sourcePath);
    }

}
