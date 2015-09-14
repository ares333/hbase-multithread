package net.phpdr.hbase.multi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * hbase 多线程Get。
 *
 * @author Ares admin@phpdr.net
 * @warning 任务池已经排重，但是已经执行或执行中的任务无法排重。
 */
public class HGet extends HBaseMulti {
	private final int perThreadNum = 1000;
	private HashSet<String> taskPool = new HashSet<String>();

	public HGet(String table, String[] columns, int threadNum) throws Exception {
		super(table, columns, threadNum);
	}

	public void addTask(String rowkey) {
		synchronized (this.taskPool) {
			if (null != rowkey) {
				this.taskPool.add(rowkey);
			}
		}
	}

	public void addTask(HashSet<String> rowkeys) {
		synchronized (this.taskPool) {
			if (null != rowkeys) {
				this.taskPool.addAll(rowkeys);
			}
		}
	}

	@Override
	protected int getTaskPoolSize() {
		return this.taskPool.size();
	}

	// 开启一个新的线程
	@Override
	protected synchronized void threadStart() throws IOException {
		if (!taskPool.isEmpty()) {
			int num = taskPool.size();
			if (perThreadNum < num) {
				num = perThreadNum;
			}
			String[] task = new String[num];
			Iterator<String> it = taskPool.iterator();
			while (it.hasNext()) {
				task[--num] = it.next();
				it.remove();
				if (num == 0) {
					break;
				}
			}
			HBaseThread thread = new HThread(task);
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

	// get线程
	class HThread extends HBaseThread {
		private String[] keys;

		public HThread(String[] keys) {
			this.keys = keys;
		}

		@Override
		public void process() throws Exception {
			HTable table = (HTable) HBaseMulti.getHTablePool().getTable(
					HGet.this.table);
			ArrayList<Get> list = new ArrayList<Get>(this.keys.length + 1);
			HashMap<String, String> row = null;
			Get get = null;
			KeyValue[] cols = null;
			Result results[] = null;
			for (String key : keys) {
				get = new Get(Bytes.toBytes(key));
				if (null == HGet.this.columns) {
					get.addFamily(Bytes.toBytes(cf));
				} else {
					if (HGet.this.columns.length == 0) {
						FilterList f = new FilterList();
						f.addFilter(new KeyOnlyFilter());
						// 因为是基于列，所以如果不指定所有列名也会一并返回
						f.addFilter(new FirstKeyOnlyFilter());
						get.setFilter(f);
						f = null;
					} else {
						String cfCol[];
						for (String colName : HGet.this.columns) {
							cfCol = colName.split(":", 2);
							if (cfCol.length == 1) {
								get.addColumn(Bytes.toBytes(cf),
										Bytes.toBytes(colName));
							} else {
								get.addColumn(Bytes.toBytes(cfCol[0]),
										Bytes.toBytes(cfCol[1]));
							}
						}

					}
				}
				list.add(get);
			}
			results = table.get(list);
			for (Result result : results) {
				cols = result.raw();
				row = new HashMap<String, String>(cols.length + 1, 2);
				for (KeyValue col : cols) {
					row.put(Bytes.toString(col.getQualifier()),
							Bytes.toString(col.getValue()));
				}
				row.put("__rowkey", Bytes.toString(result.getRow()));
				if (null != row.get("__rowkey"))
					HGet.this.buffer.push(row);
				synchronized (HGet.this.counter) {
					HGet.this.counter++;
				}
			}
			row = null;
			list = null;
			results = null;
			table = null;
		}
	}
}