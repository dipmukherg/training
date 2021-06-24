package com.training.springml;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
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
        final Pipeline pipeline = Pipeline.create(options);
        FileReader fileReader = new FileReader();
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
