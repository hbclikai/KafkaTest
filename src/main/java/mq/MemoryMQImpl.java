package mq;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 用BlockingQueue实现的内存消息队列
 */
public class MemoryMQImpl<T> implements MemoryMQ<T> {
	private BlockingQueue<T> queue;

	public MemoryMQImpl() {
		// 如果不指定大小,队列的最大大小就是Integer.MAX_VALUE
		queue = new LinkedBlockingQueue<T>();
	}

	public MemoryMQImpl(int capacity) {
		// 指定队列的最大大小
		queue = new LinkedBlockingQueue<T>(capacity);
	}

	@Override
	public boolean put(T t) {
		try {
			queue.put(t);
		} catch (InterruptedException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	@Override
	public T get() {
		T t = null;
		try {
			t = queue.take();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return t;
	}

	@Override
	public int size() {
		return queue.size();
	}

	@Override
	public void restore(T t) {
		try {
			queue.put(t);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}