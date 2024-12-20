package org.dspbench.applications.spikedetection;

import org.dspbench.base.constants.BaseConstants;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public interface SpikeDetectionConstants extends BaseConstants {
    String PREFIX = "sd";
    
    interface Config extends BaseConfig {
        String PARSER_VALUE_FIELD       = "sd.parser.value_field";
        String GENERATOR_COUNT          = "sd.generator.count";
        String MOVING_AVERAGE_THREADS   = "sd.moving_average.threads";
        String MOVING_AVERAGE_WINDOW    = "sd.moving_average.window";
        String SPIKE_DETECTOR_THREADS   = "sd.spike_detector.threads";
        String SPIKE_DETECTOR_THRESHOLD = "sd.spike_detector.threshold";
    }
    
    interface Field {
        String DEVICE_ID = "deviceID";
        String TIMESTAMP = "timestamp";
        String VALUE = "value";
        String MOVING_AVG = "movingAverage";
        String MESSAGE = "message";
    }
    
    interface Component extends BaseComponent {
        String MOVING_AVERAGE = "movingAverageOperator";
        String SPIKE_DETECTOR = "spikeDetectorOperator";
    }
    
    interface Streams {
        String SENSORS  = "sensorStream";
        String AVERAGES = "averageStream";
        String SPIKES   = "spikeStream";
    }
}
