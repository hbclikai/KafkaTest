package producer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import mq.MemoryMQ;
import mq.MemoryMQImpl;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerWithMq {
	private static KafkaProducer<String, String> producer;
	private static final String TOPIC = "topic1";
	static {
		Properties conf = new Properties();
		// 如果是多个,就写成"192.168.1.1:9092,192.168.1.2:9092,192.168.1.3:9092"这样
		conf.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.159.150:9092");
		conf.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		conf.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		conf.put(ProducerConfig.ACKS_CONFIG, "1");
		producer = new KafkaProducer<String, String>(conf);
	}

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		// step1.准备一个mq,按顺序放进去10000个String
		MemoryMQ<String> mq = new MemoryMQImpl<>();
		for (int i = 0; i < 10000; i++) {
			mq.put("ProducerWithMq" + i);
		}

		// step2.由于是循环, 变量放在外面声明
		ProducerRecord<String, String> record = null;
		Future<RecordMetadata> future = null;
		String currentMessage = null;
		boolean isSend = true; // 上一次发送是否成功

		// step3.从消息队列中取内容,然后发送
		// 经过多次试验(拔网线,挂起虚拟机),这种方式在网络出错的情况下,有重复写入的消息,但是不丢消息
		// 原因就是,已经send的消息,get的时候出错,程序认为没写进去,但kafka给写进去了
		while (true) {
			if (isSend) { // 上次发送成功,才从mq中读,否则,还是发送上次的内容
				currentMessage = mq.get(); // 阻塞
			}
			record = new ProducerRecord<String, String>(TOPIC, null, null, currentMessage);
			try {
				future = producer.send(record); // 默认是异步发送
				future.get(); // 如果报错,就是没有成功
				System.out.println(currentMessage + "已经写入");
				isSend = true;
			} catch (Exception e) {
				e.printStackTrace();
				isSend = false;
			}
		}
	}
}
