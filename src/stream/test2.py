from xmlrpc import server
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from beam_nuggets.io import kafkaio
import apache_beam.transforms.window as window
import re
from apache_beam.transforms import trigger

consumer_config_1 = {"topic": "topic_test",
                   "bootstrap_servers": "localhost:9092",
                   "group_id": "view streaming data",
                   "auto_offset_reset": "latest"}

consumer_config_2 = {"topic": "topic_test_2",
                   "bootstrap_servers": "localhost:9092",
                   "group_id": "view streaming data 2"
                   }


def run_pipeline():
  options = PipelineOptions()
  with beam.Pipeline(options=options) as p:
    (p
     | 'Reading product views from Kafka' >> kafkaio.KafkaConsume(consumer_config=consumer_config_1, value_decoder=bytes.decode)
     | 'Split words 2' >> beam.Map(lambda words: (re.sub('[^0-9]','',str(words).split(",")[2]), 2))
     #| 'Window of 10 seconds' >> beam.WindowInto(windowfn=window.FixedWindows(5),
     #                                            trigger=trigger.Repeatedly(underlying=),
     #                                            accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
     #                                            allowed_lateness=window.Duration.of(0)
     #                                           )
     | 'Window of 10 seconds' >> beam.WindowInto(window.GlobalWindows())
     #| 'keys' >> beam.Keys()
     | 'get 2' >> beam.CombineGlobally(beam.combiners.ToDictCombineFn()).without_defaults()
     #| 'get 3' >> beam.MapTuple(lambda k, v: (k, sum(v)))
     | 'print' >> beam.Map(print)
     #| 'map another' >> beam.Map(lambda kv: (str(None), str(str(kv[0])+"_"+str(kv[1]))))
     #| 'combine per key' >> beam.GroupByKey()
     #| 'summing' >> beam.Map(lambda kv: (kv[0], str(sum(kv[1]))))
     #| 'Print to console' >> beam.Map(print)
     #| 'pushing to kafka' >> kafkaio.KafkaProduce(topic='topic_test_2', servers='localhost:9092')
    )

if __name__ == '__main__':
  run_pipeline()
        

