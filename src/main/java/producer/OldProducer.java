package producer;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

@SuppressWarnings("deprecation")
public class OldProducer {
	private final Producer<String, String> producer;
	public final static String TOPIC = "topic5";

	public OldProducer() {
		Properties props = new Properties();
		// 此处配置的是kafka的端口
		props.put("metadata.broker.list", "192.168.159.150:9092");
		props.put("zk.connect", "192.168.159.150:2181");
		// 配置value的序列化类
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		// 配置key的序列化类
		props.put("key.serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");
		// 同步发送还是异步发送,速度相差非常大
		props.put("producer.type", "sync");
		producer = new Producer<String, String>(new ProducerConfig(props));
	}

	public void produce(String value) {
		producer.send(new KeyedMessage<String, String>(TOPIC, value));
	}

	public static void main(String[] args) {
		OldProducer p = new OldProducer();
		for (int i = 0;; i++) {
			String value = "data" + i;
			p.produce(value);
			if (i % 1000 == 0) {
				System.out.println("已发送:" + i);
			}
		}
	}
}
