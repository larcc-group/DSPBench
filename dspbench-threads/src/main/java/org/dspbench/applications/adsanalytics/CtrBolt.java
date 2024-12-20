package org.dspbench.applications.adsanalytics;

import org.dspbench.base.operator.BaseOperator;
import org.dspbench.core.Tuple;
import org.dspbench.core.Values;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author mayconbordin
 */
public class CtrBolt extends BaseOperator {
    private Map<String, Summary> summaries;

    @Override
    protected void initialize() {
        summaries = new HashMap<String, Summary>();
    }

    @Override
    public void process(Tuple tuple) {
        AdEvent event = (AdEvent) tuple.getValue(AdsAnalyticsConstants.Field.EVENT);
        
        String key = String.format("%d:%d", event.getQueryId(), event.getAdID());
        Summary summary = summaries.get(key);
        
        // create summary if it don't exists
        if (summary == null) {
            summary = new Summary();
            summaries.put(key, summary);
        }
        
        // update summary
        if (tuple.getStreamId().equals(AdsAnalyticsConstants.Streams.CLICKS)) {
            summary.clicks++;
        } else {
            summary.impressions++;
        }
        
        // calculate ctr
        double ctr = (double)summary.clicks / (double)summary.impressions;
        
        emit(tuple, new Values(event.getQueryId(), event.getAdID(), ctr));
    }
    
    private static class Summary {
        public long impressions = 0;
        public long clicks = 0;
    }
}
