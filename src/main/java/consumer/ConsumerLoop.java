package consumer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
/**
 * 出处:http://blog.csdn.net/opensure/article/details/72419701
 * 在这个的基础上精简的
 * 经测试
 * 1)本段代码在断网的情况下仍能运行,在断网处会有几个重复消费
 * 2)开启两个相同的本段代码,一个崩溃了,另一个自动运行,但是不能同时运行
 */
public class ConsumerLoop implements Runnable {
	private final KafkaConsumer<String, String> consumer;
	private final List<String> topics;
	private final int id;

	public ConsumerLoop(int id, String groupId, List<String> topics) {
		this.id = id;
		this.topics = topics;
		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.159.150:9092");
		props.put("group.id", groupId);
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", StringDeserializer.class.getName());
		this.consumer = new KafkaConsumer<>(props);
	}

	public void run() {
		try {
			consumer.subscribe(topics);
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
				for (ConsumerRecord<String, String> record : records) {
					Map<String, Object> data = new HashMap<>();
					data.put("partition", record.partition());
					data.put("offset", record.offset());
					data.put("value", record.value());
					System.out.println(this.id + ": " + data);
				}
			}
		} catch (Exception e) {
			// ignore for shutdown
		} finally {
			consumer.close();
		}
	}
	
	public static void main(String[] args) {
		int numConsumers = 3;
		String groupId = "consumer-tutorial-group2";
		List<String> topics = Arrays.asList("topic1");
		ExecutorService executor = Executors.newFixedThreadPool(numConsumers);
		for (int i = 0; i < numConsumers; i++) {
			ConsumerLoop consumer = new ConsumerLoop(i, groupId, topics);
			executor.submit(consumer);
		}
	}
}


