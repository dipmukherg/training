import os
import time 
from google.cloud import pubsub_v1

if __name__ == "__main__":
    # Replace  with your pubsub topic
    pubsub_topic = 'projects/myspringml2/topics/training-input'

    # Replace  with your input file path
    input_file = 'store_sales.csv'

    # create publisher
    publisher = pubsub_v1.PublisherClient()

    with open(input_file, 'rb') as ifp:
        # skip header
        header = ifp.readline()  
        
        # loop over each record
        for line in ifp:
            event_data = line   # entire line of input CSV is the message
            print('Publishing {0} to {1}'.format(event_data, pubsub_topic))
            publisher.publish(pubsub_topic, event_data)
            time.sleep(1)    