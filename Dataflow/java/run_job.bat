set filepattern=gs://springml-training/store_sales*.csv
set bucket=gs://springml-training
set outputpubsubtopic=projects/myspringml2/topics/training-dataflow-java
mvn compile exec:java -Dexec.mainClass=com.training.springml.GcsToPubsub -Dexec.args="--inputFilePattern=/store_sales*.csv --inputFileLocation=gs://springml-training --outputPubsubTopic=projects/myspringml2/topics/training-dataflow-java --watchesNewFilesIntervalSec=20 --runner=DataflowRunner --project=myspringml2 --region=us-central1 --gcpTempLocation=gs://springml-training/dataflow_java/temp --stagingLocation=gs://springml-training/dataflow_java/staging"