import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import com.fasterxml.jackson.databind.{ DeserializationFeature, ObjectMapper }
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Increment
import org.apache.hadoop.hbase.util.Bytes

object StreamAOT {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  val hbaseConf: Configuration = HBaseConfiguration.create()
  hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
  
  // Configure HBase to work with hosts on the cluster
  hbaseConf.set("hbase.zookeeper.quorum","mpcs53014c10-m-6-20191016152730.us-central1-a.c.mpcs53014-2019.internal")
  hbaseConf.set("zookeeper.znode.parent", "/hbase-unsecure")
  
  // Connect to HBase and get table to increment
  val hbaseConnection = ConnectionFactory.createConnection(hbaseConf)
  val weatherHourly = hbaseConnection.getTable(TableName.valueOf("jonathantan_weather_this_hour"))
  
  def getPastWeather(node_vsn: String) = {
    val result = weatherHourly.get(new Get(Bytes.toBytes(node_vsn)))
    if (result.isEmpty())
      None
    else
      Some(NodeRecord(
        node_vsn,
        Integer.valueOf(Bytes.toString(result.getValue(Bytes.toBytes("stats"), Bytes.toBytes("day")))),
        Bytes.toDouble(result.getValue(Bytes.toBytes("stats"), Bytes.toBytes("hum_this_hour"))),
        Bytes.toDouble(result.getValue(Bytes.toBytes("stats"), Bytes.toBytes("hum_today"))),
        Bytes.toLong(result.getValue(Bytes.toBytes("stats"), Bytes.toBytes("hum_latest"))),
        Integer.valueOf(Bytes.toString(result.getValue(Bytes.toBytes("stats"), Bytes.toBytes("month")))),
        Bytes.toDouble(result.getValue(Bytes.toBytes("stats"), Bytes.toBytes("temp_this_hour"))),
        Bytes.toDouble(result.getValue(Bytes.toBytes("stats"), Bytes.toBytes("temp_today"))),
        Bytes.toLong(result.getValue(Bytes.toBytes("stats"), Bytes.toBytes("temp_latest")))
      ))
  }
  
  // Function to increment HBase table for each new record received from Kafka
  def incrementWeatherByNode(kor: KafkaObsRecord) : String = {
 
    // Get current record
    val nodeRecordResponse = getPastWeather(kor.nodeVSN)
    if (nodeRecordResponse.isEmpty)
      return "No data for node " + kor.nodeVSN
    val nodeRecord = nodeRecordResponse.get
    
    // Build put call 
    val inc = new Increment(Bytes.toBytes(kor.nodeVSN))
    if (kor.sensorPath == "metsense.tmp112.temperature") {
      val diff = kor.sensorValue - nodeRecord.temp_latest;
      inc.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("temp_latest"), diff)
    }
    if (kor.sensorPath == "metsense.htu21d.humidity") {
      val diff = kor.sensorValue - nodeRecord.hum_latest;
      inc.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("hum_latest"), diff) 
    }
    
    // Send put call
    weatherHourly.increment(inc)
    return "Updated data for node " + kor.nodeVSN
  }
  
  // Main control flow
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println(s"""
        |Usage: StreamFlights <brokers> 
        |  <brokers> is a list of one or more Kafka brokers
        | 
        """.stripMargin)
      System.exit(1)
    }
    
    val Array(brokers) = args

    // Create context with 5 minute batch interval
    val sparkConf = new SparkConf().setAppName("StreamFlights")
    val ssc = new StreamingContext(sparkConf, Seconds(5 * 60))

    // Create direct kafka stream with brokers and topics
    val topicsSet = Set("jonathantan_weather")
    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
   
    // Get value from (key, value) for each Kafka message and convert to KafkaObsRecord
    val serializedRecords = messages.map(_._2);
    val kfrs = serializedRecords.map(rec => mapper.readValue(rec, classOf[KafkaObsRecord]))

    // Apply incrementing function for each Kafka message and print result
    val processedNodeUpdate = kfrs.map(incrementWeatherByNode)
    processedNodeUpdate.print()
    
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

}