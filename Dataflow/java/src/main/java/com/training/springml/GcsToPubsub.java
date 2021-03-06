package com.training.springml;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Watch;
import org.joda.time.Duration;


public class GcsToPubsub {
    public static void main(String[] args) {
        GcsToPubSubOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(GcsToPubSubOptions.class);
        run(options);
    }

    private static void run(GcsToPubSubOptions options) {
        Pipeline pipeline = Pipeline.create(options);

        /*
         * Steps:
         *  1) Read from the text source.
         *  2) Write each text record to Pub/Sub
         */
        pipeline
                .apply("Read Text Data", TextIO.read().from(options.getInputFilePattern())
                .watchForNewFiles(Duration.millis(600000), Watch.Growth.never()))
                .apply("Write to PubSub", PubsubIO.writeStrings().to(options.getOutputPubsubTopic()));
        pipeline.run();
    }



}
