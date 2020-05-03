package jonathantan.KafkaIngestAOT;

public class KafkaObsRecord {
	public KafkaObsRecord(String timestamp, String nodeVSN, String sensorPath, Long sensorValue) {
		super();
		
		this.timestamp = timestamp;
		this.nodeVSN = nodeVSN;
		this.sensorPath = sensorPath;
		this.sensorValue = sensorValue;
	}

	public String getTimestamp() {
		return timestamp;
	}
	
	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}
	
	public String getNodeVSN() {
		return nodeVSN;
	}
	
	public void setNodeVSN(String nodeVSN) {
		this.nodeVSN = nodeVSN;
	}
	
	public String getSensorPath() {
		return sensorPath;
	}
	
	public void setSensorPath(String sensorPath) {
		this.sensorPath = sensorPath;
	}
	
	public Long getSensorValue() {
		return sensorValue;
	}
	
	public void setSensorValue(Long sensorValue) {
		this.sensorValue = sensorValue;
	}
	
	private String timestamp;
	private String nodeVSN;
	private String sensorPath;
	private Long sensorValue;
	
}
