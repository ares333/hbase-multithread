package net.phpdr.hbase.multi;

import java.util.HashMap;

/**
 * HBase多线程类用到的缓冲区
 *
 * @author Ares admin@phpdr.net
 */
public class HBuffer {
	// 指向最后一个数据
	private int index = -1;
	// 指示是否还有数据写入
	private boolean signalTerminate = false;
	private boolean suspend = false;
	private HashMap<String, String>[] list;

	@SuppressWarnings("unchecked")
	public HBuffer(int num) {
		this.list = new HashMap[num];
	}

	public synchronized int size() {
		return this.index + 1;
	}

	/**
	 * 如果缓冲区已经terminate会重新启用
	 *
	 * @param item
	 * @throws Exception
	 */
	public synchronized void push(HashMap<String, String> item)
			throws Exception {
		// 如果用if的话被唤醒则不会再进行下标判断，从而可能抛出java.lang.ArrayIndexOutOfBoundsException
		while (this.index == this.list.length - 1 || this.suspend) {
			this.wait();
		}
		if (true == this.signalTerminate) {
			this.signalTerminate = false;
		}
		this.list[++this.index] = item;
		this.notify();
	}

	/**
	 * 如果缓冲区为空并且已经terminate则总是返回null
	 *
	 * @return
	 * @throws InterruptedException
	 */
	public synchronized HashMap<String, String> pop()
			throws InterruptedException {
		// 如果用if的话被唤醒则不会再进行下标判断，从而可能抛出java.lang.ArrayIndexOutOfBoundsException
		while (this.index < 0 || this.suspend) {
			if (this.signalTerminate) {
				return null;
			} else {
				this.wait();
			}
		}
		this.notify();
		return this.list[this.index--];
	}

	/**
	 * 暂停缓冲区，也就是间接暂停了监视器上的所有线程
	 */
	public synchronized void suspend() {
		this.suspend = true;
	}

	/**
	 * 从暂停状态恢复缓冲区
	 */
	public synchronized void resume() {
		this.suspend = false;
		this.notifyAll();
	}

	/**
	 * 表示不再往缓冲区写入更多数据了
	 */
	public synchronized void terminate() {
		this.signalTerminate = true;
		this.notifyAll();
	}
}