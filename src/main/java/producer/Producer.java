package producer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class Producer {
	private static KafkaProducer<String, String> producer;
	private static final String TOPIC = "topic6";
	static {
		Properties conf = new Properties();
		// 如果是多个,就写成"192.168.1.1:9092,192.168.1.2:9092,192.168.1.3:9092"这样
		conf.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.159.150:9092");
		conf.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		conf.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		producer = new KafkaProducer<String, String>(conf);
	}

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		ProducerRecord<String, String> record = null;
		Future<RecordMetadata> send = null;
		long start = System.currentTimeMillis();
		for (int i = 0; i < 100; i++) {
			record = new ProducerRecord<String, String>(TOPIC, null, null, "value" + i);
			send = producer.send(record); // 默认是异步发送
			// Thread.sleep(1);
		}
		send.get();
		long end = System.currentTimeMillis();
		System.out.println("耗费时间:" + (end - start));
	}
}
