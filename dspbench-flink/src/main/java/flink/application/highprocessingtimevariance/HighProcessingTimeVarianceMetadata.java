package flink.application.highprocessingtimevariance;

import java.io.Serializable;

public class HighProcessingTimeVarianceMetadata implements Serializable { 
    private int eventId;
    private int duration;

    public HighProcessingTimeVarianceMetadata(int eventId, int duration) {
        this.eventId = eventId;
        this.duration = duration;
    }

    public int getEventId() {
        return this.eventId;
    }

    public int getDuration() {
        return this.duration;
    }

    @Override
    public String toString() {
        return "HighProcessingTimeVarianceMetadata(" + "eventId=" + this.eventId + ", duration=" + this.duration + ")";
    }
}