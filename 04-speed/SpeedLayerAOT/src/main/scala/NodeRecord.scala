case class NodeRecord(
    nodeVSN: String,
    day: Int,
    hum_this_hour: Double,
    hum_today: Double,
    hum_latest: Long,
    month: Int,
    temp_this_hour: Double,
    temp_today: Double,
    temp_latest: Long
 )