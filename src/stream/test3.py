import apache_beam as beam
import apache_beam.transforms.window as window

with beam.Pipeline() as pipeline:
  produce_counts = (
      pipeline
      | 'Create produce counts' >> beam.Create([
          ('spring', '🍓'),
          ('spring', '🥕'),
          ('spring', '🍆'),
          ('spring', '🍅'),
          ('summer', '🥕'),
          ('summer', '🍅'),
          ('summer', '🌽'),
          ('fall', '🥕'),
          ('fall', '🍅'),
          ('winter', '🍆'),
      ])
      | 'Window of 10 seconds' >> beam.WindowInto(window.FixedWindows(10))
      | 'Group counts per produce' >> beam.GroupByKey()
      | beam.MapTuple(lambda k, vs: (k, sorted(vs)))  # sort and format
      | beam.Map(print))