package flink.application.highprocessingtimevariance;

import java.io.Serializable;
import java.util.List;
import java.util.ArrayList;


public class HighProcessingTimeVarianceEvent implements Serializable { 
    private int id;

    // Synthetic data
    private List<Integer> durations;

    // Generated data
    private List<HighProcessingTimeVarianceMetadata> metadatas;

    public HighProcessingTimeVarianceEvent(int id) {
        this.id = id;
        this.durations = new ArrayList();
        this.metadatas = new ArrayList();
    }

    public HighProcessingTimeVarianceEvent(int id, List<Integer> durations) {
        this.id = id;
        this.durations = durations;
        this.metadatas = new ArrayList();
    }

    public int getId() {
        return this.id;
    }

    public List<Integer> getDurations() {
        return this.durations;
    }

    public List<HighProcessingTimeVarianceMetadata> getMetadatas() {
        return this.metadatas;
    }

    @Override
    public String toString() {
        return "HighProcessingTimeVarianceEvent(" + "id=" + this.id + ")";
    }
}
