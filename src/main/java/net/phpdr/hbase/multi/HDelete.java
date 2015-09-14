package net.phpdr.hbase.multi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * hbase 多线程Delete。
 *
 * @author Ares admin@phpdr.net
 * @warning 数据源线程的个数在外部确定。
 */
public class HDelete extends HBaseMulti {
	private final int perThreadNum = 1000;
	private List<Thread> taskPool = Collections
			.synchronizedList(new ArrayList<Thread>());
	private boolean enableStatus = true;

	public HDelete(String table, int threadNum) throws Exception {
		super(table, null, threadNum);
	}

	/**
	 * 添加一个Callable实例，如果call()返回null表示没有更多数据
	 *
	 * @param task
	 */
	public void addTask(final Callable<HashMap<String, String>> task) {
		class AddTaskThread extends Thread {
			@Override
			public void run() {
				try {
					HashMap<String, String> row = null;
					for (row = task.call(); row != null; row = task.call()) {
						if (!row.isEmpty()) {
							HDelete.this.buffer.push(row);
						}
					}
					HDelete.this.taskPool.remove(this);
					if (HDelete.this.taskPool.isEmpty()) {
						HDelete.this.buffer.terminate();
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

	public void enableStatus(boolean enable) {
		this.enableStatus = enable;
	}

	@Override
	protected int getTaskPoolSize() {
		return this.taskPool.size();
	}

	// put线程始终保持一个threadNum的并发
	@Override
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
			}
		}
	}

	class HThread extends HBaseThread {
		@Override
		public void process() throws Exception {
			HTable table = (HTable) HBaseMulti.getHTablePool().getTable(
					HDelete.this.table);
			ArrayList<Delete> list = new ArrayList<Delete>(
					HDelete.this.perThreadNum + 1);
			HashMap<String, String> row = null;
			while (true) {
				row = HDelete.this.buffer.pop();
				if (null != row && row.containsKey("__rowkey")) {
					Delete delete = new Delete(Bytes.toBytes(row
							.get("__rowkey")));
					list.add(delete);
					// 单加是为了status函数模运算操作
					synchronized (HDelete.this.counter) {
						HDelete.this.counter++;
					}
					if (HDelete.this.enableStatus)
						HDelete.this.status();
				}
				if (list.size() == HDelete.this.perThreadNum || row == null) {
					table.delete(list);
					list.clear();
					if (row == null) {
						break;
					}
				}
			}
			table = null;
		}
	}
}