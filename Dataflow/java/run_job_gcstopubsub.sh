export filepattern=gs://springml-training/store_sales*.csv
export bucket=gs://springml-training
export outputpubsubtopic=projects/myspringml2/topics/training-dataflow-java
mvn compile exec:java -Dexec.mainClass=com.training.springml.GcsToPubsub \
-Dexec.args="--inputFilePattern=${filepattern} \
--inputFileLocation=${bucket} \
--outputPubsubTopic=${outputpubsubtopic}  \
--watchesNewFilesIntervalSec=20" -Pdirect-runner