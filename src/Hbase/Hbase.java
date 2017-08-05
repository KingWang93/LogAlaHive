package Hbase;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import com.sun.xml.internal.messaging.saaj.soap.MultipartDataContentHandler;

import util.DateTime;
import util.HbaseUtil;

public class Hbase {
    private static HbaseUtil hu = null;
    
    public static void main(String[] args){
    	hu = new HbaseUtil("Master,Slave1,Slave2", "2181","192.168.182.133:60000");
        String table_name = "tomcat_access_log";
        try {
			if(!hu.tableExist(table_name)){
				String s_time = DateTime.getCurrentTime();
				String[] family_names = new String[]{"accessinfo","details"}; 
				hu.create(table_name, family_names);
				TableName tableName = TableName.valueOf(table_name);
		        //表对象
		        Table table = hu.getconn().getTable(tableName);
				File root = new File("F:/KingWang/postgraduate/Cloud Compute/课程演讲/Tomcat 日志/logs");
				SimpleDateFormat df = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
				SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
				File[] files = root.listFiles();
				int i=0;
				for (File file : files) {
					if (file.isFile() && file.exists() && file.getName().contains("localhost_access_log")) { // 判断文件是否存在
						List<Put> list = new ArrayList<>();
						InputStreamReader read = new InputStreamReader(new FileInputStream(file), "GBK");// 考虑到编码格式
						BufferedReader bufferedReader = new BufferedReader(read);
						String lineTxt = null;
						while ((lineTxt = bufferedReader.readLine()) != null) {
							Pattern p = Pattern.compile("(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})\\s-\\s-\\s\\[(.*)\\s.*\\]\\s\\\"(GET|POST)\\s([^?]+)\\??.*\\s(.*)\\\"\\s(\\d*)\\s(\\d*)");
							Matcher matcher = p.matcher(lineTxt);
							if (matcher.find()) {
								if (matcher.group(4).length() > 100) {
									continue;
								}else{
									i++;
									String s1 = matcher.group(1);//IP
									String s2 = matcher.group(2);//时间
									String new_s2 = String.valueOf(Long.MAX_VALUE-df.parse(s2).getTime());//key
									String date_s2 = sdf.format(df.parse(s2));//eg.20170117
									String s3 = matcher.group(3);//访问方法
									String s4 = matcher.group(4);//URL
									String s5 = matcher.group(5);//protocol
									String s6 = matcher.group(6);//状态
									String s7 = matcher.group(7);//接收字节数
									 // put对象 负责录入数据
							        Put put = new Put(new_s2.getBytes());
							        put.addColumn(family_names[0].getBytes(), "ip".getBytes(), s1.getBytes());
							        put.addColumn(family_names[0].getBytes(), "day".getBytes(), date_s2.getBytes());
							        put.addColumn(family_names[0].getBytes(), "method".getBytes(), s3.getBytes());
							        put.addColumn(family_names[1].getBytes(), "url".getBytes(), s4.getBytes());
							        put.addColumn(family_names[1].getBytes(), "protocol".getBytes(), s5.getBytes());
							        put.addColumn(family_names[1].getBytes(), "status".getBytes(), s6.getBytes());
							        put.addColumn(family_names[1].getBytes(), "bytes".getBytes(), s7.getBytes());
							        list.add(put);
									System.out.println("正在put...."+String.valueOf(i));
								}
								
							}
						}
						read.close();
						table.put(list);
					} else {
						System.out.println("找不到指定的文件");
					}
				}
				System.out.println("put结束!");
				hu.close();
				String e_time = DateTime.getCurrentTime();
				System.out.println("开始时间:");
				System.out.println(s_time);
				System.out.println("结束时间:");
				System.out.println(e_time);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
}