package net.phpdr.hbase.multi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * hbase 多线程Put。 HBase有一个高效的批量加载（bulk loading）工具，比HBase API写入速度至少快一个数量级
 * http://hbase.apache.org/docs/current/bulk-loads.html
 * 
 * @author xiepeng@joyport.com/aresrr@126.com
 * @warning 数据源线程的个数在外部确定。
 */
public class HPut extends HBaseMulti {
	private final int perThreadNum = 1000;
	private List<Thread> taskPool = Collections
			.synchronizedList(new ArrayList<Thread>());
	private boolean enableStatus = true;

	public HPut(String table, int threadNum) throws Exception {
		super(table, null, threadNum);
	}

	/**
	 * 添加一个Callable的实例，如果call()返回null表示没有更多数据
	 * 
	 * @param task
	 */
	public void addTask(final Callable<HashMap<String, String>> task) {
		class AddTaskThread extends Thread {
			public void run() {
				try {
					HashMap<String, String> row = null;
					for (row = task.call(); row != null; row = task.call()) {
						if (!row.isEmpty()) {
							HPut.this.buffer.push(row);
						}
					}
					HPut.this.taskPool.remove(this);
					if (HPut.this.taskPool.isEmpty()) {
						HPut.this.buffer.terminate();
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		AddTaskThread thread = new AddTaskThread();
		this.taskPool.add(thread);
		thread.start();
	}

	public HashMap<String, String> fetch() throws InterruptedException {
		return null;
	}

	public void enableStatus(boolean enable) {
		this.enableStatus = enable;
	}

	protected int getTaskPoolSize() {
		return this.taskPool.size();
	}

	/**
	 * put线程始终保持一个threadNum的并发
	 */
	protected synchronized void threadStart() throws IOException {
		if (!taskPool.isEmpty()) {
			HBaseThread thread = new HThread();
			this.threadList.add(thread);
			thread.start();
			thread = null;
		} else {
			// 是否结束了？
			if (this.threadList.isEmpty()) {
				System.out.print(this.statusStr());
				// 如果调用了block()则唤醒
				notify();
			}
		}
	}

	/**
	 * 阻塞，直到任务结束
	 * 
	 * @throws InterruptedException
	 */
	public synchronized void block() throws InterruptedException {
		wait();
	}

	class HThread extends HBaseThread {
		public void process() throws Exception {
			HTable table = (HTable) HBaseMulti.getHTablePool().getTable(
					HPut.this.table);
			table.setAutoFlush(false, false);
			ArrayList<Put> list = new ArrayList<Put>(HPut.this.perThreadNum + 1);
			HashMap<String, String> row = null;
			while (true) {
				row = HPut.this.buffer.pop();
				if (null != row && row.containsKey("__rowkey")) {
					Put put = new Put(Bytes.toBytes(row.get("__rowkey")));
					row.remove("__rowkey");
					for (String col : row.keySet()) {
						if (null == row.get(col)) {
							row.put(col, "");
						}
						put.add(Bytes.toBytes(cf), Bytes.toBytes(col),
								Bytes.toBytes(row.get(col)));
					}
					list.add(put);
					// 单加是为了status函数模运算操作
					synchronized (HPut.this.counter) {
						HPut.this.counter++;
					}
					if (HPut.this.enableStatus)
						HPut.this.status();
				}
				if (list.size() == HPut.this.perThreadNum || row == null) {
					table.put(list);
					list.clear();
					if (row == null) {
						break;
					}
				}
			}
			if (HPut.this.threadList.isEmpty()) {
				HBaseMulti.getHTablePool().getTable(HPut.this.table)
						.flushCommits();
			}
			table = null;
		}
	}
}