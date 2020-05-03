import scala.reflect.runtime.universe._

case class KafkaObsRecord(
    timestamp: String,
    nodeVSN: String,
    sensorPath: String,
    sensorValue: Long)