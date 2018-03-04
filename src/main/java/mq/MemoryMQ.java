package mq;
/**
 * 内存消息队列,是第一级缓存
 */
public interface MemoryMQ<T> {
	public boolean put(T t); 

	public T get();

	public int size();

	/**
	 * 如果取走这条消息后,无法保存到下一个地方,就恢复这条消息
	 */
	public void restore(T t);
}
