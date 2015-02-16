package edu.cjh.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;


public class HBaseTest {
	public static Configuration configuration;
	public static HTablePool pool;
	static{
		configuration=HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.property.clientPort", "2222");
		configuration.set("hbase.zookeeper.quorum", "192.168.80.3");
		configuration.set("hbase.master","192.168.80.3:9000");
		pool=new HTablePool(configuration,1000);
	}
	
	public static void main(String args[]){
//		createTable("test");
//		insertData("test");
//		dropTable("test");
//		deleteRow("test","112233bbbccc");
//		queryAll("student");
//		queryByCondition1("test","112233bbbccc");
		queryByCondition2("test","aaa");
	}
	
	/**
	 * 创建表
	 * @param tableName
	 */
	public static void createTable(String tableName){
		System.out.println("start create table....");
		try {
			HBaseAdmin hBaseAdmin=new HBaseAdmin(configuration);
			if(hBaseAdmin.tableExists(tableName)){
				hBaseAdmin.disableTable(tableName);
				hBaseAdmin.deleteTable(tableName);
				System.out.println(tableName+"is exist,delete....");
			}
			HTableDescriptor tableDescriptor=new HTableDescriptor(tableName);
			tableDescriptor.addFamily(new HColumnDescriptor("column1"));
			tableDescriptor.addFamily(new HColumnDescriptor("column2"));
			tableDescriptor.addFamily(new HColumnDescriptor("column3"));
			hBaseAdmin.createTable(tableDescriptor);
			System.out.println("end create table....");
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	/**
	 * 插入数据
	 * @param tableName
	 */
	public static void insertData(String tableName){
		System.out.println("start insert data....");
		HTable table=(HTable)pool.getTable(tableName);
		Put put=new Put("112233bbbcccc".getBytes());
		put.add("column1".getBytes(), null,"aaa".getBytes());
		put.add("column2".getBytes(),null,"bbb".getBytes());
		put.add("column3".getBytes(),null,"ccc".getBytes());
		try {
			table.put(put);
			System.out.println("end insert data....");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	/**
	 * 删除表
	 * @param tableName
	 */
	public static void dropTable(String tableName){
		try {
			HBaseAdmin hBaseAdmin=new HBaseAdmin(configuration);
			if(hBaseAdmin.tableExists(tableName)){
				hBaseAdmin.disableTable(tableName);
				hBaseAdmin.deleteTable(tableName);
				System.out.println(tableName+" has been deleted...");
			}else{
				System.out.println(tableName+" does not exist...");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	/**
	 * 根据rowKey删除一条记录
	 * @param tableName
	 * @param rowKey
	 */
	public static void deleteRow(String tableName,String rowKey){
		try {
			HTable table=new HTable(configuration,tableName);
			Delete delete=new Delete(rowKey.getBytes());
			table.delete(delete);
			System.out.println("delete row record success");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	/**
	 * 查询所有数据
	 * @param tableName
	 */
	public static void queryAll(String tableName){
		HTable table=(HTable)pool.getTable(tableName);
		ResultScanner rs=null;
		try {
			rs=table.getScanner(new Scan());
			for(Result r:rs){
				System.out.println("get rowKey:"+new String(r.getRow()));
				for(KeyValue keyValue:r.raw()){
					System.out.println("raw:"+new String(keyValue.getFamily())
					+"===value:"+new String(keyValue.getValue()));
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			if(rs!=null){
				rs.close();
			}
		}
	}
	/**
	 * 单条件查询，根据rowKey查询唯一一条记录
	 * @param tableName
	 * @param rowKey
	 */
	public static void queryByCondition1(String tableName,String rowKey){
		HTable table=(HTable)pool.getTable(tableName);
		System.out.println("According rowKey:"+rowKey);
		Get get=new Get(rowKey.getBytes());
		try {
			Result	r=table.get(get);
			for(KeyValue keyValue:r.raw()){
				System.out.println("raw:"+new String(keyValue.getFamily())
				+"===value："+new String(keyValue.getValue()));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	/**
	 * 单条件查询，查询多条记录
	 * @param tableName
	 * @param rowKey
	 */
	public static void queryByCondition2(String tableName,String key){
		HTable table=(HTable)pool.getTable(tableName);
		Filter filter=new SingleColumnValueFilter(Bytes.toBytes("column1"), null, CompareOp.EQUAL, Bytes.toBytes(key));
		Scan scan=new Scan();
		scan.setFilter(filter);
		ResultScanner rs=null;
		try {
			rs=table.getScanner(scan);
			for(Result r:rs){
				System.out.println("rowKey:"+new String(r.getRow()));
				for(KeyValue keyValue:r.raw()){
					System.out.println("raw:"+new String(keyValue.getFamily())+"===value:"+new String(keyValue.getValue()));
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			if(rs!=null){
				rs.close();
			}
		}
	}
	//组合查询
}
