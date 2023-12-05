'''
 UNUSED 

'''

# importing the libraries
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from beam_nuggets.io import kafkaio
import apache_beam.transforms.window as window

# consumer configuration
# topic -> productview
consumer_config = {"topic": "productview",
                   "bootstrap_servers": "localhost:9092",
                   "group_id": "view streaming data"}

# regex to only get the relevant data
regex = r'0-9\,'

# pipeline to process the data
with beam.Pipeline(options=PipelineOptions()) as p:
    product_view = p | "Reading product views from Kafka" >> kafkaio.KafkaConsume(
        consumer_config=consumer_config,
        #value_decoder=bytes.decode,  # optional
    )
    #product_view | 'split the tokens' >> beam.FlatMap(lambda vals: str(vals).split(','))
    product_view | 'Mapping' >> beam.KvSwap()
    product_view | 'Window of 10 seconds' >> beam.WindowInto(window.FixedWindows(5))
    product_view | 'Calculating views' >> beam.Map(print)


#product_view | 'get only values' >> beam.Keys()
#product_view | 'get only values' >> beam.Map(print)
#product_view | 'Summing' >> beam.Map(lambda kv: (kv[0], sum(int(kv[1]))))
#product_view | 'parse through regex' >> beam.Regex.matches(regex)
    