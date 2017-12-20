package com.ppcredit.sql.ppcredit;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.ppcredit.hqlparse.JobHistory;
import com.ppcredit.hqlparse.StackData;

public class Test2 {
	public static void main(String[] args) throws ParseException {
	
		String[] str=new String[]{"2016-10-26 00:01:16","liumiao"};
		String[] strs = new String[]{"2016-10-26 00:01:16"};
		//	HttpResponse.stamToDate("1477411276693");
		 System.out.println(strs.length);
		defineUrl(str);
		defineUrl(strs);
	
		
		
		
		
		
	}	
	
	 public static String dateToStamp(String s) throws ParseException{
	        String res;
	        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	        Date date = simpleDateFormat.parse(s);
	        long ts = date.getTime();
	        res = String.valueOf(ts);
	        System.out.println(res);
	        return res;
	    }
	 
	 public static String defineUrl(String[] args) throws ParseException{
		 String str ="http://192.168.213.22:19888/ws/v1/history/mapreduce/jobs";
			String url =null;
			StringBuffer sb = new StringBuffer(str);
			if(args.length==0){	
				url = str;	
			}else if(args.length==1){
				url= sb.append("?").append("startTime="+dateToStamp(args[0])).toString();		
			}else if(args.length==2){
				url=sb.append("?").append("startTime="+dateToStamp(args[0])).append("&user="+args[1]).toString();
			}
			 System.out.println(url);
			 return url;
	    } 
	 
}