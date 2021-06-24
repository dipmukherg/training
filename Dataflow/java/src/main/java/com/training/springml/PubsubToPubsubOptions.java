package com.training.springml;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

public interface PubsubToPubsubOptions extends  PipelineOptions{

    @Description("Pub/Sub subscription to read the input from. Should be in format: projects/<project>/subscriptions/<subscription>")
    ValueProvider<String> getInputPubsubSubscription();
    void setInputPubsubSubscription(ValueProvider<String> value);

    @Description("Pub/Sub topic to write output to. Should be in format: projects/<project>/topics/<topic>")
    String getOutputPubsubTopic();
    void setOutputPubsubTopic(String value);

}
