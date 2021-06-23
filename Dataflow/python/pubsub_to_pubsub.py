import apache_beam as beam
import argparse
import logging
import csv
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.options.pipeline_options import SetupOptions

from apache_beam import window
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, AccumulationMode, AfterCount

def encode_byte_string(element):
   element = str(element)
   return element.encode('utf-8')

def custom_timestamp(elements):
  unix_timestamp = elements[7]
  return beam.window.TimestampedValue(elements, int(unix_timestamp))

def calculateProfit(elements):
  buy_rate = elements[5]
  sell_price = elements[6]
  products_count = int(elements[4])
  profit = (int(sell_price) - int(buy_rate)) * products_count
  elements.append(str(profit))
  return elements

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--inputSubscription',
                        dest='inputSubscription',
                        help='input subscription name',
                        default='projects/myspringml2/subscriptions/training-input-sub'
                        )
    parser.add_argument('--outputTopic',
                        dest='outputTopic',
                        help='output topic name',
                        default='projects/myspringml2/topics/training-op1'
                        )
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=pipeline_options) as p:
        pubsub_data = (
                    p 
                    | 'Read from pub sub' >> beam.io.ReadFromPubSub(subscription= known_args.inputSubscription)
                    | 'Remove extra chars' >> beam.Map(lambda data: (data.rstrip().lstrip()))
                    | 'Split Row' >> beam.Map(lambda row : row.decode().split(','))
                    | 'Filter By Country' >> beam.Filter(lambda elements : (elements[1] == "Mumbai" or elements[1] == "Bangalore"))
                    | 'Create Profit Column' >> beam.Map(calculateProfit)
                    | 'Apply custom timestamp' >> beam.Map(custom_timestamp)
                    | 'Form Key Value pair' >> beam.Map(lambda elements : (elements[0], int(elements[8])))
                    # Please change the time of gap of window duration and period accordingly
                    | 'Window' >> beam.WindowInto(window.SlidingWindows(30,10))
                    | 'Sum values' >> beam.CombinePerKey(sum)
                    | 'Encode to byte string' >> beam.Map(encode_byte_string)
                    | 'Write to pus sub' >> beam.io.WriteToPubSub(known_args.outputTopic)
                )
        
        p.run().wait_until_finish()
        

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()