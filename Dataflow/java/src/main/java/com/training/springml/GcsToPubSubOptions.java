package com.training.springml;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;

public interface GcsToPubSubOptions extends GcsOptions{
    @Validation.Required
    @Description("The file pattern to read records from (e.g. gs://bucket/file-*.csv)")
    String getInputFilePattern();

    void setInputFilePattern(String value);

    @Description("The file pattern to read records from (e.g. gs://bucket)")
    String getInputFileLocation();
    void setInputFileLocation(String value);

    @Description("Pub/Sub topic to write output to. Should be in format: projects/<project>/topics/<topic>")
    String getOutputPubsubTopic();
    void setOutputPubsubTopic(String value);

    @Default.Long(30L)
    @Description("Watches for new files at the given interval in seconds. Default: 60")
    Long getWatchesNewFilesIntervalSec();
    void setWatchesNewFilesIntervalSec(Long value);
}
