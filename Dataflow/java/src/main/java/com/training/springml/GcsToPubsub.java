package com.training.springml;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.ReadableFileCoder;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Duration;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class GcsToPubsub {
    public static void main(String[] args) {
        GcsToPubSubOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(GcsToPubSubOptions.class);
        run(options);
    }

    private static void run(GcsToPubSubOptions options) {
        final Pipeline pipeline = Pipeline.create(options);
        FileReader fileReader = new FileReader();

        PDone processed = pipeline
                .apply("File Loading", fileReader)
                .apply("File Reading", new ReadSplittableFile())
                .apply("Row Processing", new Processor("training-user","gcs-to-pubsub"))
                .apply("Success Messages Write", PubsubIO.writeMessages().to(options.getOutputPubsubTopic()));

        pipeline.run();
    }



}

class FileReader extends PTransform<PBegin, PCollection<FileIO.ReadableFile>> {

    @Override
    public PCollection<FileIO.ReadableFile> expand(PBegin input) {
        final GcsToPubSubOptions options = input.getPipeline().getOptions().as(GcsToPubSubOptions.class);
        PCollection<FileIO.ReadableFile> pipeline = input
                .apply("GCS Files Listening", FileIO.match()
                        .filepattern(options.getInputFileLocation().concat(options.getInputFilePattern()))
                        .continuously(Duration.standardSeconds(options.getWatchesNewFilesIntervalSec()), Watch.Growth.never())
                )
                .apply("File Reading", FileIO.readMatches());
        return pipeline;
    }
}


class Processor extends PTransform<PCollection<String>, PCollection<PubsubMessage>>{
    private String user;
    private String job;

    public Processor(String user,String job){
        this.user=user;
        this.job=job;
    }

    @Override
    public PCollection<PubsubMessage> expand(PCollection<String> input) {
        return input
                .apply(convert(user,job));
    }

    private ParDo.SingleOutput<String, PubsubMessage> convert(String user,String job){
        return ParDo.of(new DoFn<String, PubsubMessage>() {

            private Map<String, String> attributes = new HashMap<>();
            {
                attributes.put("User", user != null ? user : "");
                attributes.put("Job", job != null ? job : "");
            }

            @ProcessElement
            public void process(ProcessContext c) {
                String line = c.element();
                c.output(convertToPubsub(line,attributes));
            }

        });
    }

    private PubsubMessage convertToPubsub(String line, Map<String, String> attributes){
        return new PubsubMessage(line.getBytes(StandardCharsets.UTF_8), attributes);
    }
}

class ReadSplittableFile extends PTransform<PCollection<FileIO.ReadableFile>, PCollection<String>>{

    @Override
    public PCollection<String> expand(PCollection<FileIO.ReadableFile> input) {
        return input
                .apply("Map Elements",ParDo.of(
                        new DoFn<FileIO.ReadableFile, String>() {
                            @ProcessElement
                            public void process(ProcessContext c) throws IOException {
                                FileIO.ReadableFile file=c.element();
                                c.output(file.readFullyAsUTF8String());
                            }
                        }));
    }
}
