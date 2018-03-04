package producer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import mq.MemoryMQ;
import mq.MemoryMQImpl;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerMultiThread {
	private static KafkaProducer<String, String> producer;
	private static final String TOPIC = "topic1";
	private static final int THREADS_NUMS = 5;
	
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
			mq.put("value" + i);
		}
		
		// step2.创建线程池,启动多个线程向kafka中写入
		ExecutorService executor = Executors.newFixedThreadPool(THREADS_NUMS);
		for (int i = 0; i < THREADS_NUMS; i++) {
			ProducerThread thread = new ProducerThread("kafka_write_thread_" + THREADS_NUMS, producer, TOPIC, mq);
			executor.submit(thread);
		}
	}
}

class ProducerThread extends Thread {
	private KafkaProducer<String, String> producer;
	private String topic;
	private MemoryMQ<String> mq;
	private boolean isShutdown = false;

	public ProducerThread(String threadName, KafkaProducer<String, String> producer, String topic, MemoryMQ<String> mq) {
		super(threadName);
		this.producer = producer;
		this.topic = topic;
		this.mq = mq;
	}

	/**
	 * 停止此线程
	 */
	public void shutdown() {
		this.isShutdown = true;
	}

	@Override
	public void run() {
		// step1.由于是循环, 变量放在外面声明
		ProducerRecord<String, String> record = null;
		Future<RecordMetadata> future = null;
		String currentMessage = null;
		boolean isSend = true; // 上一次发送是否成功

		while (true) {
			// step2.判断本线程有没有被停止
			if (isShutdown) { // 如果被停止了
				if (!isSend) { // 如果上一次没写成功,就先恢复这条消息
					mq.restore(currentMessage);
				}
				break;
			}
			// step3.判断一下如果上次发送成功,才从mq中读,否则,还是发送上次的内容
			if (isSend) {
				currentMessage = mq.get(); // 阻塞
			}

			// step4.真的发送
			record = new ProducerRecord<String, String>(topic, null, null, currentMessage);
			try {
				future = producer.send(record); // 默认是异步发送
				future.get(); // 阻塞,如果报错,就是没有成功
				System.out.println(currentMessage + "已经写入-" + Thread.currentThread().getName());
				isSend = true;
				Thread.sleep(1000);
			} catch (Exception e) {
				e.printStackTrace();
				isSend = false; // 如果写入失败,就标记一下
			}
		}
	}
}
