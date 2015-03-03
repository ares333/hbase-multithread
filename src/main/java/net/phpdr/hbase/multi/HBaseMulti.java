package net.phpdr.hbase.multi;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;

/**
 * 
 * HBase多线程基类V2.6, rowkey统一以"__rowkey"为键
 * 
 * @author Ares admin@phpdr.net
 */
public abstract class HBaseMulti {
	private static HTablePool hTablePool;
	// 最大并发不能超过缓冲区(bufferSize)大小，否则可能导致外部线程无限等待！
	protected static final int threadLimit = 100;
	protected static final int bufferSize = 10000;
	protected final String table;
	protected final String[] columns;
	// 用户设置的并发数
	protected final int threadNum;
	protected List<Thread> threadList = Collections
			.synchronizedList(new ArrayList<Thread>());
	protected final HBuffer buffer;
	// 列簇名
	protected String cf = "cf";
	protected Integer counter = 0;
	private Integer statusCounter = 0;
	// 用于控制所有线程结束后最后一次状态输出
	protected boolean showStatus = false;

	public HBaseMulti(String table, String[] columns, int threadNum)
			throws Exception {
		this.table = table;
		this.columns = columns;
		if (threadNum > HBaseMulti.threadLimit)
			threadNum = threadLimit;
		this.threadNum = threadNum;
		this.buffer = new HBuffer(bufferSize);
	}

	/**
	 * <pre>
	 * 设置列簇的名称，默认为cf。
	 * 构造方法的列名中指定的列簇优先级最高（只在HScan和HGet中有效）
	 * </pre>
	 * 
	 * @param cf
	 */
	public void setCf(String cf) {
		this.cf = cf;
	}

	/**
	 * 返回HTablePool
	 * 
	 * @return
	 */
	protected static HTablePool getHTablePool() {
		if (null == HBaseMulti.hTablePool) {
			Configuration conf = HBaseConfiguration.create();
			conf.set("hbase.zookeeper.quorum",
					"10.135.69.130,10.135.69.132,10.135.69.134,10.135.69.136");
			conf.set("hbase.zookeeper.property.clientPort", "2181");
			conf.set(HConstants.HBASE_REGIONSERVER_LEASE_PERIOD_KEY, "1800000");
			conf.set(HConstants.HBASE_RPC_TIMEOUT_KEY, "3600000");
			HBaseMulti.hTablePool = new HTablePool(conf, HBaseMulti.threadLimit);
		}
		return HBaseMulti.hTablePool;
	}
	
	/**
	 * 获取一个HBase表
	 * 
	 * @param name
	 *            String
	 * @return HTable
	 */
	public static HTable getHTable(String name) {
		return (HTable) getHTablePool().getTable(name);
	}

	/**
	 * 取一个结果出来，如果返回null表示没有更多内容可以读取了。
	 */
	public HashMap<String, String> fetch() throws InterruptedException {
		return this.buffer.pop();
	}

	/**
	 * 此方法为了性能没有同步，所以不一定是1000行一显示，this.counter命中1000整倍数和处理速度有极大关系，
	 * 如果处理速度过慢可能根本不显示状态。
	 */
	public void status() {
		if ((counter - this.statusCounter) > 1000) {
			if (this.showStatus == false)
				this.showStatus = true;
			String msg = this.statusStr();
			System.out.print(msg);
			this.statusCounter = counter;
		}
	}

	/**
	 * 状态字符串，此方法为了性能没有同步，所以显示的是一个比较准确的近似数
	 * 
	 * @return String
	 */
	protected String statusStr() {
		// 计算一下读线程的waiting，blocked个数
		int waitingNum = 0;
		int blockedNum = 0;
		int threadNum = 0;
		int statusMaxLen = 0;
		// 因为迭代操作是通过对对像集的调用间接操作原对像，所以在迭代时要对迭代的对像实现再同步
		synchronized (this.threadList) {
			Iterator<Thread> it = this.threadList.iterator();
			while (it.hasNext()) {
				Thread.State state = it.next().getState();
				if (state == Thread.State.WAITING) {
					waitingNum++;
				} else if (state == Thread.State.BLOCKED) {
					blockedNum++;
				}
				threadNum++;
			}
		}
		String msg = String.format("\r%s%10s%10s/%s/%s%10s", this.counter,
				this.buffer.size(), threadNum, waitingNum, blockedNum,
				this.getTaskPoolSize());
		if (msg.length() > statusMaxLen) {
			statusMaxLen = msg.length();
		}
		msg = String.format("\r%" + statusMaxLen + "s", " ") + msg;
		return msg;
	}

	/**
	 * 开始执行任务
	 * 
	 * @throws Exception
	 */
	public void fuck() throws Exception {
		for (int i = 0; i < this.threadNum; i++) {
			this.threadStart();
		}
	}

	/**
	 * 开启一个新的线程，该方法必须是同步的！
	 * 
	 * @throws Exception
	 */
	protected abstract void threadStart() throws Exception;

	/**
	 * 一个用于状态显示的函数就不用进行同步了
	 * 
	 * @return int
	 */
	protected abstract int getTaskPoolSize();

	/**
	 * scan线程
	 */
	abstract class HBaseThread extends Thread {
		public void run() {
			try {
				this.process();
				HBaseMulti.this.threadList.remove(this);
				HBaseMulti.this.threadStart();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		public abstract void process() throws Exception;
	}
}
