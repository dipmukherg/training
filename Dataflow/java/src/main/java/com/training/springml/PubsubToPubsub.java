package com.training.springml;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Watch;
import org.joda.time.Duration;

public class PubsubToPubsub {

    public static void main(String[] args) {
        PubsubToPubsubOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(PubsubToPubsubOptions.class);
        run(options);
    }
    private static void run(PubsubToPubsubOptions options) {
        Pipeline pipeline = Pipeline.create(options);

        /*
         * Steps:
         *  1) Read from pub/sub subscription.
         *  2) Write each text record to Pub/Sub
         */
        pipeline
                .apply("Read from Pubsub", PubsubIO.readStrings().fromSubscription(options.getInputPubsubSubscription()))
                .apply("Write to PubSub", PubsubIO.writeStrings().to(options.getOutputPubsubTopic()));
        pipeline.run();
    }

}
