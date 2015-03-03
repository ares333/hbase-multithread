package net.phpdr.hbase.multi;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * hbase 多线程扫描。
 * 
 * @author xiepeng@joyport.com/aresrr@126.com
 * @warning taskPool并没有做区间排重，所以避免重复扫描需要在外部保证。
 */
public class HScan extends HBaseMulti {
	private final int scanCache = 1000;
	private Scan demoScan;// 自定义scan，可对scan做一些自定义设置
	private ArrayList<String[]> taskPool = new ArrayList<String[]>();

	public HScan(String table, String[] columns, int threadNum)
			throws Exception {
		super(table, columns, threadNum);
	}

	/**
	 * 设置自定义scan，多线程启动scan时都以这个为模板 可以设置一些scan参数，比如filter,timestamp
	 */
	public void setScan(Scan scan) {
		this.demoScan = scan;
	}

	/**
	 * 
	 * @param start
	 *            string 开始rowkey，inclusive
	 * @param end
	 *            string 结束rowkey，exclusive
	 * @param num
	 *            int 开始和结束rowkey平分成num份
	 * @throws Exception
	 */
	public void addTask(String start, String end, int num) throws Exception {
		synchronized (this.taskPool) {
			// 先验证start是否小于等于end
			if (null != start && null != end && this.isNumeric(start)
					&& this.isNumeric(end)
					&& Integer.valueOf(start) < Integer.valueOf(end) && num > 1) {
				// 必须是数字型的才能平分
				if (this.isNumeric(start) && this.isNumeric(end)) {
					int avg = (int) Math.ceil((new Double(end) - new Double(
							start)) / num);
					int left = Integer.valueOf(start);
					String[] task = null;
					for (int i = 0; i < num; i++) {
						task = new String[2];
						task[0] = String.valueOf(left);
						if (left >= Integer.valueOf(end))
							break;
						left += avg;
						task[1] = String.valueOf(left);
						if (Integer.valueOf(task[1]) >= Integer.valueOf(end)) {
							task[1] = end;
						}
						this.taskPool.add(task);
					}
				} else {
					throw new Exception("Start and end is invalid");
				}
			} else {
				String[] task = { new String(start), new String(end) };
				this.taskPool.add(task);
			}
		}
	}

	/**
	 * 判断一个字符串是否是数字组成
	 * 
	 * @param str
	 *            string
	 * @return boolean
	 */
	private boolean isNumeric(String str) {
		if (str == null || str.isEmpty()) {
			return false;
		}
		for (int i = str.length(); --i >= 0;) {
			if (!Character.isDigit(str.charAt(i))) {
				return false;
			}
		}
		return true;
	}

	protected int getTaskPoolSize() {
		return this.taskPool.size();
	}

	/**
	 * 开启一个新的线程
	 */
	protected synchronized void threadStart() throws Exception {
		if (!taskPool.isEmpty()) {
			String[] task = taskPool.remove(0);
			HBaseThread thread = new HThread(task[0], task[1]);
			this.threadList.add(thread);
			thread.start();
			thread = null;
		} else if (this.threadList.size() == 0) {
			if (this.showStatus)
				System.out.print(this.statusStr());
			this.buffer.terminate();
			HBaseMulti.getHTablePool().closeTablePool(this.table);
		}
	}

	/**
	 * hbase scan线程
	 */
	class HThread extends HBaseThread {
		private String startKey;
		private String endKey;

		public HThread(String startKey, String endKey) {
			this.startKey = startKey;
			this.endKey = endKey;
		}

		public void process() throws Exception {
			HTable table = (HTable) HBaseMulti.getHTablePool().getTable(
					HScan.this.table);
			Scan scan;
			if (demoScan != null) {
				scan = new Scan(demoScan);
			} else {
				scan = new Scan();
			}
			if (null == columns) {
				scan.addFamily(Bytes.toBytes(cf));
			} else {
				if (HScan.this.columns.length == 0) {
					FilterList f = new FilterList();
					f.addFilter(new KeyOnlyFilter());
					// 因为是基于列，所以如果不指定所有列名也会一并返回
					f.addFilter(new FirstKeyOnlyFilter());
					if (scan.getFilter() != null)
						f.addFilter(scan.getFilter());
					scan.setFilter(f);
					f = null;
				} else {
					String cfCol[];
					for (String colName : HScan.this.columns) {
						cfCol = colName.split(":", 2);
						if (cfCol.length == 1) {
							scan.addColumn(Bytes.toBytes(cf),
									Bytes.toBytes(colName));
						} else {
							scan.addColumn(Bytes.toBytes(cfCol[0]),
									Bytes.toBytes(cfCol[1]));
						}
					}
				}
			}
			if (null != this.startKey)
				scan.setStartRow(Bytes.toBytes(String.valueOf(this.startKey)));
			if (null != this.endKey)
				scan.setStopRow(Bytes.toBytes(String.valueOf(this.endKey)));
			scan.setCaching(HScan.this.scanCache);
			ResultScanner resultScanner = table.getScanner(scan);
			scan = null;
			KeyValue[] cols = null;
			Result result = null;
			HashMap<String, String> row = null;
			for (result = resultScanner.next(); result != null; result = resultScanner
					.next()) {
				// KeyOnlyFilter()使col.getValue()没有值
				if (HScan.this.columns != null
						&& HScan.this.columns.length == 0) {
					row = new HashMap<String, String>(1, 2);
				} else {
					cols = result.raw();
					row = new HashMap<String, String>(cols.length + 1, 2);
					for (KeyValue col : cols) {
						row.put(Bytes.toString(col.getQualifier()),
								Bytes.toString(col.getValue()));
						col = null;
					}
				}
				row.put("__rowkey", Bytes.toString(result.getRow()));
				HScan.this.buffer.push(row);
				synchronized (HScan.this.counter) {
					HScan.this.counter++;
				}
				row = null;
			}
			cols = null;
			resultScanner.close();
			resultScanner = null;
		}
	}
}