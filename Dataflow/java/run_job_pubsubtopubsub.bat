mvn compile exec:java -Dexec.mainClass=com.training.springml.PubsubToPubsub -Dexec.args="--inputPubsubSubscription=projects/myspringml2/subscriptions/training-input-sub --outputPubsubTopic=projects/myspringml2/topics/training-dataflow-java --jobName=pubsub-to-pubsub --runner=DataflowRunner --project=myspringml2 --region=us-central1 --gcpTempLocation=gs://springml-training/dataflow_java/pubsubtopubsub/temp --stagingLocation=gs://springml-training/dataflow_java/pubsubtopubsub/staging"