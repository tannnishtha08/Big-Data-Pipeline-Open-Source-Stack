// bin/spark-shell --packages "com.datastax.spark:spark-cassandra-connector_2.12:3.0.0","org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.1"

import org.apache.spark.streaming._
import org.apache.spark.SparkContext
import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.TopicPartition
import java.time

val sc = SparkContext.getOrCreate

val ssc = new StreamingContext(sc, Seconds(10))

val preferredHosts = LocationStrategies.PreferConsistent

val topics = List("productview")

val kafkaParams = Map(
  "bootstrap.servers" -> "localhost:9092",
  "key.deserializer" -> classOf[StringDeserializer],
  "value.deserializer" -> classOf[StringDeserializer],
  "group.id" -> "spark-streaming-notes",
  "auto.offset.reset" -> "earliest"
)

val offsets = Map(new TopicPartition("productview", 0) -> 2L)

val dstream = KafkaUtils.createDirectStream[String, String](
  ssc,
  preferredHosts,
  ConsumerStrategies.Subscribe[String, String](topics, kafkaParams, offsets))

dstream.map(record=>(record.value().toString.split(",")(1).toString.substring(0,record.value().toString.split(",")(1).toString.length()-1),1)).reduceByKey( _ + _ ).map{case (k,v) => (k,v); (k,v,time.LocalDateTime.now().toString)}.saveToCassandra("ecommerce", "product_view", SomeColumns("product_id","view_count","created_at"))

ssc.start
