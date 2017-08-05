package util;

import java.io.IOException;

import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
/**
 * hbase for version 1.1.1
 * @author Administrator
 *
 */
public class HbaseUtil {
    public static final String ZK_QUORUM  = "hbase.zookeeper.quorum";
    public static final String ZK_CLIENTPORT  = "hbase.zookeeper.property.clientPort";
    public static final String HMASTER = "hbase.master";  
    private Configuration conf = HBaseConfiguration.create();
    private Connection connection ;
    private Admin admin;
    
    public HbaseUtil(String zk_quorum,String zk_clientPort,String h_master) {
        conf.set(ZK_QUORUM, zk_quorum);
        conf.set(ZK_CLIENTPORT, zk_clientPort);
        conf.set(HMASTER, h_master);
        init();
    }
    
    public Connection getconn(){
    	return connection;
    }
    public Configuration getconf(){
    	return conf;
    }

    private void init(){
        try {
            //Connection 的创建是个重量级的工作，线程安全，是操作hbase的入口
            connection = ConnectionFactory.createConnection(conf);
            admin  = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void close(){
        try {
            if(admin != null) admin.close();
            if(connection!=null) connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * 创建一个表
     * @param table_name 表名称
     * @param family_names 列族名称集合
     * @throws IOException 
     */
    public void create(String table_name,String... family_names) throws IOException{
        //获取TableName
        TableName tableName = TableName.valueOf(table_name);
        //table 描述
        HTableDescriptor htabledes =  new HTableDescriptor(tableName);
        for(String family_name : family_names){
            //column 描述
            HColumnDescriptor family = new HColumnDescriptor(family_name);
            htabledes.addFamily(family);
        }
        admin.createTable(htabledes);
    }
    /**
     * 增加一条记录
     * @param table_name 表名称
     * @param row    rowkey
     * @param family 列族名称
     * @param qualifier 列族限定符(可以为null)
     * @param value 值
     * @throws IOException
     */
    public void addColumn(String table_name,String row, String family,String qualifier,String value) throws IOException{
        //表名对象
        TableName tableName = TableName.valueOf(table_name);
        //表对象
        Table table = connection.getTable(tableName);
        // put对象 负责录入数据
        Put put = new Put(row.getBytes());
        put.addColumn(family.getBytes(), qualifier.getBytes(), value.getBytes());
        table.put(put);
    }
    /**
     * 判断表是否存在
     */
    public boolean tableExist(String table_name) throws IOException{
        return admin.tableExists(TableName.valueOf(table_name));
    }
    /**删除表*/
    public void deleteTable(String table_name) throws IOException{
        TableName tableName = TableName.valueOf(table_name);
        if(admin.tableExists(tableName)){
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
    }
    /**
     * 查询单个row的记录
     * @param table_name 表明
     * @param row  行键
     * @param family  列族
     * @param qualifier  列族成员
     * @return
     * @throws IOException
     */
    public Cell[] getRow(String table_name,String row,String family,String qualifier) throws IOException{
        Cell[] cells = null;
        //check
        if(StringUtils.isEmpty(table_name)||StringUtils.isEmpty(row)){
            return null;
        }
        //Table
        Table table = connection.getTable(TableName.valueOf(table_name));
        Get get = new Get(row.getBytes());
        //判断在查询记录时,是否限定列族和子列(qualifier).
        if(StringUtils.isNotEmpty(family)&&StringUtils.isNotEmpty(qualifier)){
            get.addColumn(family.getBytes(), qualifier.getBytes());
        }
        if(StringUtils.isNotEmpty(family)&&StringUtils.isEmpty(qualifier)){
            get.addFamily(family.getBytes());
        }
        Result result = table.get(get);
        cells = result.rawCells();
        return cells;
    }
    /**
     * 获取表中的所有记录,可以指定列族,列族成员,开始行键,结束行键.
     * @param table_name
     * @param family
     * @param qualifier
     * @param startRow
     * @param stopRow
     * @return
     * @throws IOException
     */
    public ResultScanner getScan(String table_name,String family,String qualifier,String startRow,String stopRow) throws IOException{
        ResultScanner resultScanner = null;
        
        //Table
        Table table = connection.getTable(TableName.valueOf(table_name));
        Scan scan = new Scan();
        if(StringUtils.isNotBlank(family)&& StringUtils.isNotEmpty(qualifier)){
            scan.addColumn(family.getBytes(), qualifier.getBytes());
        }
        if(StringUtils.isNotEmpty(family)&& StringUtils.isEmpty(qualifier)){
            scan.addFamily(family.getBytes());
        }
        if(StringUtils.isNotEmpty(startRow)){
            scan.setStartRow(startRow.getBytes());
        }
        if(StringUtils.isNotEmpty(stopRow)){
            scan.setStopRow(stopRow.getBytes());
        }
        resultScanner = table.getScanner(scan);
        
        return resultScanner;
    }
}