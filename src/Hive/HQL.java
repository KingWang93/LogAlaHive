package Hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import net.sf.json.JSONArray;

public class HQL {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	private static String url = "jdbc:hive2://192.168.182.133:10000/default";
	private static Connection conn = null;
	private static String user = "";
	private static String password = "";
	private static String sql = "";
	private static ResultSet res;
	
	public static void conn(){
		try {
			Class.forName(driverName);
			conn = DriverManager.getConnection(url, user, password);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	
	public static void close(){
		try {
			conn.close();
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	
	public static void create() {
		try {
			conn();
			Statement stmt = conn.createStatement();
			String drop_sql = "drop table access_log";
			stmt.execute(drop_sql);
			System.out.println("删除表成功！");
			String sql = "CREATE EXTERNAL TABLE IF NOT EXISTS access_log(key string,ip string,t_time string,method string,url string,protocol string,status string,bytes int) STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES (\"hbase.columns.mapping\" = \":key#s,accessinfo:ip,accessinfo:day,accessinfo:method,details:url,details:protocol,details:status,details:bytes\") TBLPROPERTIES (\"hbase.table.name\" = \"tomcat_access_log\")";
			stmt.execute(sql);
			System.out.println("表创建成功！");
			String index_sql="create index access_log_index on table access_log(ip) as 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler' with deferred rebuild IN TABLE access_log_index_table";
			stmt.execute(index_sql);
			String alter_sql = "alter index access_log_index on access_log rebuild";
			stmt.execute(alter_sql);
			close();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	public static void getIpAccess(){
		conn();
		try {
			Statement stmt = conn.createStatement();
			String query_sql = "select ip,count(ip) as num from access_log group by ip order by num desc";
			ResultSet res=stmt.executeQuery(query_sql);
			while(res.next()){
				String ip = res.getString(1);
				String count = res.getString(2);
				System.out.println(ip+"    "+count);
			}
			close();
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args){
		getIpAccess();
	}
}
