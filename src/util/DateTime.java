package util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateTime
{
	/*
	 * 获取当前时间
	 */
	public static String getCurrentTime()
	{
		Date date=new Date();
		SimpleDateFormat sd=new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		String currentTime=sd.format(date);
		return currentTime;
	}
	/*
	 * 获取某个时间的年
	 * @para	date	日期
	 */
	public static String getYear(Date date)
	{
		SimpleDateFormat sd=new SimpleDateFormat("yyyy");
		String year=sd.format(date);
		return year;
	}
	/*
	 * 获取某个时间的月
	 * @para	date	日期
	 */
	public static String getMonth(Date date)
	{
		SimpleDateFormat sd=new SimpleDateFormat("MM");
		String month=sd.format(date);
		return month;
	}
	/*
	 * 获取某个时间的天
	 * @para	date	日期
	 */
	public static String getDay(Date date)
	{
		SimpleDateFormat sd=new SimpleDateFormat("dd");
		String day=sd.format(date);
		return day;
	}
	/*
	 * 根据某个时间往前推24小时
	 * @para	dateStirng	时间
	 */
	public static String getPast24Time(String dateStirng)
	{
		SimpleDateFormat sd=new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date past24date=new Date();
		try
		{
			past24date=new Date(sd.parse(dateStirng).getTime()-24*3600*1000);
		}catch(ParseException ex)
		{
			ex.printStackTrace();
		}
		String past24String=sd.format(past24date);
		return past24String;
	}
	/*
	 * 根据某个时间获取当天的凌晨
	 * @para	dateStirng	时间
	 */
	public static String getNoonNightTime(String dateStirng)
	{
		String dateNoonString=dateStirng.split(" ")[0]+" 00:00:00";
		return dateNoonString;
	}
}