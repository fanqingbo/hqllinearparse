package com.ppcredit.hqlparse;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import org.apache.hadoop.hive.ql.parse.HiveParser.ifExists_return;
import org.stringtemplate.v4.compiler.STParser.args_return;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class Test02 {
	
	

	
	public static String dateToStamp(String s) throws ParseException{
	        String res;
	        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(StackData.HR_TIMEFORMAT);
	        Date date = simpleDateFormat.parse(s);
	        long ts = date.getTime();
	        res = String.valueOf(ts);
	        System.out.println(res);
	        return res;
	}
	
	public static String defineUrl(String[] strs) throws ParseException{
		 String str =StackData.HR_FIRURL;
			String url =null;
			StringBuffer sb = new StringBuffer(str);
			if(strs.length==0){	
				url = str;	
			}else if(strs.length==1){
				
				url= sb.append("?").append("startTime="+dateToStamp(strs[0])).toString();
			}else if(strs.length==2){
				url=sb.append("?").append("startTime="+dateToStamp(strs[0])).append("&user="+strs[1]).toString();
			}	
			 return url;
	    } 
	
	
	
	
	
	public static String conn(String url){
		 String result = "";
		  try{
		   String urlName = url ;
		   URL U = new URL(urlName);
		   URLConnection connection = U.openConnection();
		   connection.connect();		  
		   BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
		   String line;
		   while ((line = in.readLine())!= null){
		    result += line;
		   }
		   in.close();   
		  }catch(Exception e){			  
		  }
		return result;		  
	}	
	public static void sendGet(String url,String[] args){
//		String[] strs = new String[]{"2017-9-26 00:00:16"};
		  String result =	conn(url);
		  JSONObject  result1 =JSON.parseObject(result);
		  JSONObject jobsObject = result1.getJSONObject(StackData.HR_JOBS);
		  JSONArray jobArray = jobsObject.getJSONArray(StackData.HR_JOB);
		  JSONObject jobObject = null;		  
		  String submitTime =null;
		  String startTime = null;
		  String finishTime =null;
		  String id =null;
		  String name = null;  
		  String queue =null;
		  String user = null;
		  String state = null;
		  String mapsTotal =null;
		  String mapsCompleted =null;
		  String reducesTotal =null;
		  String reducesCompleted =null; 
		  System.out.println("---------------");
		  System.out.println("size:"+jobArray.size());
		  System.out.println("---------------");
		  
		  int v=0;

		  for (int i = 0; i < jobArray.size(); i++) {
			  jobObject = (JSONObject)jobArray.get(i);
			  try {
				
				submitTime=stamToDate(jobObject.getString(StackData.HR_SUBMITTIME));
				
				startTime= stamToDate(jobObject.getString(StackData.HR_STARTTIME));
				System.out.println(startTime);
				
				if(!startTime.equals(null)&&Long.valueOf(jobObject.getString(StackData.HR_STARTTIME))<Long.valueOf(dateToStamp(args[0]))){
					continue;
				}
				v++;
				System.out.println("submitTime:"+submitTime);
				System.out.println("startTime:"+startTime);
				finishTime =stamToDate(jobObject.getString(StackData.HR_FINISHTIME));
				System.out.println("finishTime:"+finishTime);
			} catch (ParseException e1) {
				e1.printStackTrace();
			}			
			  id =	jobObject.getString(StackData.HR_ID); 
			  
			  System.out.println("id"+id);
			  queue =jobObject.getString(StackData.HR_QUEUE);
			  System.out.println("queue:"+queue);
			  user = jobObject.getString(StackData.HR_USER);
			  System.out.println("user:"+user);
		  }
 	 	  System.out.println(v);
	}
	
	
	
	
		
	public static String stamToDate(String stam) throws ParseException{
		 SimpleDateFormat sdf = new SimpleDateFormat(StackData.HR_TIMEFORMAT);
	     Long timeLong = Long.parseLong(stam); 
	     Date date = sdf.parse(sdf.format(timeLong));
	     String dat = sdf.format(date);
	     return dat;
	}	 
	public  static String split(String  srcstring){	     	   	
	   	String url = null;
	   	if(srcstring.contains("?")){
	   		String[] str = srcstring.trim().split("\\?");     	
	    	url=str[0];
			return url;
	   	} else{
	   		url=srcstring;
	   	}
	return url;
    } 
	public static String delGroup(String str){	
		String strr = null;
		if(str!=null&&str.trim().contains(StackData.HR_GROUP)&&!str.trim().contains(StackData.HR_GROUPBY)){				
			strr = str.replace(StackData.HR_GROUP, StackData.HR_GROUPABY);		
		}else if(str!=null&&str.trim().contains(StackData.HR_GROUP)&&str.trim().contains(StackData.HR_GROUPBY)){
			strr = str.replace(StackData.HR_GROUP,StackData.HR_GROUPA).replace(StackData.HR_GROUPABY, StackData.HR_GROUPBY);		
		}else{
				strr = str;
		}
		return strr;
	}
 /**
  	* 替换原表名中的自定义的字符串(groupa)
  * @param str
  * @return
  */
	public static String replaceTableName(String str){		
		String group = null;		
		if(str!=null&&str.trim().contains(StackData.HR_GROUPA)){	
			group = str.replace(StackData.HR_GROUPA, StackData.HR_GROUP);	
		}else{
			group = str;
		}		
		return group;
	}
 /**
  	* 去掉反单引号
  * @param str
  * @return
  */
	public static String delectFanDanyin(String str){
	 
		String regexp = "\\`";
		if(str.contains(regexp)){
			String strr = str.replaceAll(regexp,"");
			return strr;
		}else {
			return str;
		}		
	}
	
	
	
	public static void main(String[] args) throws ParseException {
		
//		String[] str=new String[]{"2016-10-26 00:01:16","liumiao"};
//		
		String[] strs = new String[]{"2017-9-26 00:00:16"};
		
		String url =StackData.HR_FIRURL;
//		
//		
//		System.out.println(url);
		
		sendGet(url,strs);
		
		
	
		
		
		
		
		
	}

}
