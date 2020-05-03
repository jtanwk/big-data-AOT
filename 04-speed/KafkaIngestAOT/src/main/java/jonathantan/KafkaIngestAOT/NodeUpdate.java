package jonathantan.KafkaIngestAOT;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.sql.Timestamp;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.glassfish.jersey.jackson.JacksonFeature;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

//import jonathantan.KafkaIngestAOT.Crs;
//import jonathantan.KafkaIngestAOT.CrsProperties;
//import jonathantan.KafkaIngestAOT.Datum;
//import jonathantan.KafkaIngestAOT.Geometry;
//import jonathantan.KafkaIngestAOT.Links;
//import jonathantan.KafkaIngestAOT.Location;
//import jonathantan.KafkaIngestAOT.Meta;
import jonathantan.KafkaIngestAOT.NodeData;


public class NodeUpdate {
	static class Task extends TimerTask {
		private Client client;
		
		// API updates every 5 minutes, so get data from 5 minutes ago 
		String fiveMinAgo = new Timestamp(System.currentTimeMillis() - 5 * 60 * 1000).toString().replace(' ', 'T');

		//https://api.arrayofthings.org/api/observations?project=chicago&sensor[]=metsense.tmp112.temperature&sensor[]=metsense.htu21d.humidity
		public NodeData getNodeData() {
			Invocation.Builder bldr 
			 = client.target("https://api.arrayofthings.org/api/observations?project=chicago&timestamp=gt:"
					 + fiveMinAgo + "&sensor[]=metsense.tmp112.temperature&sensor[]=metsense.htu21d.humidity")
			 .request("application/json");
		
			try {
				return bldr.get(NodeData.class);
			} catch (Exception e) {
				System.err.println(e.getMessage());
			}
			return null;
		}

		// Send API updates to the "jonathantan_weather" topic
		Properties props = new Properties();
		String TOPIC = "jonathantan_weather";
		KafkaProducer<String, String> producer;
		
		// Taken from template
		public Task() {
			client = ClientBuilder.newClient();
			client.register(JacksonFeature.class); 
			props.put("bootstrap.servers", bootstrapServers);
			props.put("acks", "all");
			props.put("retries", 0);
			props.put("batch.size", 16384);
			props.put("linger.ms", 1);
			props.put("buffer.memory", 33554432);
			props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

			producer = new KafkaProducer<>(props);
		}

		@Override
		public void run() {
			
			// Call API 
			NodeData response = getNodeData();
			if(response == null || response.getData() == null)
				return;
			ObjectMapper mapper = new ObjectMapper();			
			
			// Process API response 
			for(Datum obs : response.getData()) {
				ProducerRecord<String, String> data;
				
				// Send API response to Kafka topic 
				try {
					// Wrap API response in KafkaObsRecord object
					KafkaObsRecord update = new KafkaObsRecord(
							obs.getTimestamp(),
							obs.getNodeVsn(),
							obs.getSensorPath(),
							Math.round(obs.getValue())
							);
					
					// Write KafkaObsRecord to ProducerRecord with project-specific topic
					data = new ProducerRecord<String, String> (
								TOPIC,
								mapper.writeValueAsString(update)
							);
					
					// Send ProducerRecord to Kafka
					producer.send(data);
							
				} catch (JsonProcessingException e) {
					System.err.println(e.getMessage());
					// e.printStackTrace();
				}
				
			}
			
		}
		
	}

	static String bootstrapServers = new String("localhost:9092");

	public static void main(String[] args) {
		if(args.length > 0)  // This lets us run on the cluster with a different kafka
			bootstrapServers = args[0];
		
		// batch size = 5 minutes
		Timer timer = new Timer();
		timer.scheduleAtFixedRate(new Task(), 0, 5*60*1000);
	}
}

