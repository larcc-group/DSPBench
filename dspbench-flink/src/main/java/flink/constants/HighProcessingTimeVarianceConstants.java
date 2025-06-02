package flink.constants;

public interface HighProcessingTimeVarianceConstants extends BaseConstants {
    String PREFIX = "hptv";

    interface Conf extends BaseConf {
        String PARSER_THREADS = "hptv.parser.threads";
        String SPLITTER_THREADS = "hptv.splitter.threads";
        String COLLECTOR_THREADS = "hptv.collector.threads";
        String REDUCER_THREADS = "hptv.reducer.threads";
    }
}
