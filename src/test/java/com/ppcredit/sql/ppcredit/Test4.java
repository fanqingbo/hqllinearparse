package com.ppcredit.sql.ppcredit;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.mapreduce.Mapper;

import com.ppcredit.hqlparse.StackData;

public class Test4 {

public static void main(String[] args) throws ParseException {
	
	
//	System.out.println(stamToDate("1477411276000"));
//	
	if (args.length==0&&false) {
		System.out.println("hello world");
		
	}
//	
//	System.out.println(stamToDate("1505811667154"));
//	
//	String r="1505811667154";
//	System.out.println(Integer.parseInt(r));
	
}
	
	
	public static String stamToDate(String stam) throws ParseException{
		 SimpleDateFormat sdf = new SimpleDateFormat(StackData.HR_TIMEFORMAT);
	     Long timeLong = Long.parseLong(stam); 
	     Date date = sdf.parse(sdf.format(timeLong));
	     String dat = sdf.format(date);
	     return dat;
	}	 
	
}
