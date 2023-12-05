import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from beam_nuggets.io import kafkaio
import apache_beam.transforms.window as window
import re
from apache_beam.transforms import trigger

consumer_config = {"topic": "topic_test",
                   "bootstrap_servers": "localhost:9092",
                   "group_id": "view streaming data"}

def run_pipeline():
  # Load pipeline options from the script's arguments
  options = PipelineOptions()
  # Create a pipeline and run it after leaving the 'with' block
  with beam.Pipeline(options=options) as p:
    # Wrap in paranthesis to avoid Python indention issues
    (p
     # Load some dummy data, this can be replaced with a proper source later on
     | "Reading product views from Kafka" >> kafkaio.KafkaConsume(consumer_config=consumer_config, value_decoder=bytes.decode)
     # Split the words into one element per word
     #| 'Split words' >> beam.FlatMap(lambda words: str(words).split(','))
     | 'Split words 2' >> beam.Map(lambda words: (re.sub('[^0-9]','',str(words).split(",")[2]), 1))
     | 'Window of 10 seconds' >> beam.WindowInto(window.FixedWindows(10))
     | 'combine per key' >> beam.CombinePerKey(sum)
     #| 'Group by key' >> beam.combiners.Count.PerKey()
     #| 'combine globally' >>  beam.GroupByKey()
     #| 'map on keys' >> beam.Map(lambda x: (x[0], sum(x[1])))
     #| 'map on keys 2' >> beam.Map(lambda x: (x))
     #| 'Print to console' >> beam.Map(lambda x: print(x))
     #| 'count per element' >> beam.MapTuple(lambda word, count: '%s: %s' % (word, count))
     # We are assigning a count of 1 to every word (very relevant if we had more data)
     # We are interested in 10 second periods of words
     #| 'Group by key' >> beam.combiners.Count.PerKey()
     #| 'Window of 10 seconds' >> beam.WindowInto(window.FixedWindows(2))
     # Group all the values (counts) of each unique word
     #| 'Pair with 2' >> beam.Map(lambda word: (str(word), 1))
     # Sum the counts for each word and return the result
     #| 'Sum word counts' >> beam.Map(lambda kv: (kv[0], sum(kv[1])))
     # Just print to the console for testing
     #| 'Window of 10 seconds' >> beam.WindowInto(window.FixedWindows(3))
     | 'Print to console' >> beam.Map(print)
    )

if __name__ == '__main__':
  run_pipeline()
        

'''
| 'Window of 10 seconds' >> beam.WindowInto(windowfn=window.FixedWindows(10),
                                            trigger=trigger.AfterCount(10),
                                            accumulation_mode=trigger.AccumulationMode.DISCARDING,
                                            allowed_lateness=20)
'''