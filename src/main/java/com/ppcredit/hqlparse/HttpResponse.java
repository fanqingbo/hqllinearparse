package com.ppcredit.hqlparse;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import java.util.Map.Entry;
import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.hive.ql.parse.HiveParser.ifExists_return;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * 主程序的接口类
 * @author fanqingbo
 *
 */
public class HttpResponse {
	
	public static void main(String[] args) throws SemanticException, org.apache.hadoop.hive.ql.parse.ParseException, ParseException{		

		
		String url =StackData.HR_FIRURL;
		
		HashMap<String,String> map =HttpResponse.sendGet(url,args);
		parse(map);	
	}	
	/**
	 * 向url发出请求，得到jobhistory数据，并存入mysql表中
	 * @param url
	 * @return
	 */
	public static HashMap<String,String> sendGet(String url,String[] args){
		if (args.length==0||args.length>2) {
			System.exit(-1);

		}
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
		  HashMap<String,String> map = new  HashMap<String,String>();
		  JDBCUtils jdbc = new JDBCUtils();	
		  ArrayList<JobHistory> list =jdbc.getAllFromJobHistory();
		  int num =-1;
		  boolean flag = false;
		  
		  
		  for (int i = 0; i < jobArray.size(); i++) {
			  jobObject = (JSONObject)jobArray.get(i);
			  try {
			
				if(Long.valueOf(jobObject.getString(StackData.HR_STARTTIME))<=Long.valueOf(dateToStamp(args[0]))){	
					
					continue;
				}
				
				submitTime=stamToDate(jobObject.getString(StackData.HR_SUBMITTIME));
				startTime= stamToDate(jobObject.getString(StackData.HR_STARTTIME));
				finishTime =stamToDate(jobObject.getString(StackData.HR_FINISHTIME));
			} catch (ParseException e1) {
				e1.printStackTrace();
			}			
			  id =	jobObject.getString(StackData.HR_ID);  			  	
			  queue =jobObject.getString(StackData.HR_QUEUE);
				  
			if(args.length==2&&!args[1].equals(jobObject.getString(StackData.HR_USER))){
				continue;				 
			}
			user = jobObject.getString(StackData.HR_USER);
				
			  mapsTotal =jobObject.getString(StackData.HR_MAPSTOTAL);
			  mapsCompleted =jobObject.getString(StackData.HR_MAPSCOMPLETED);
			  reducesTotal =jobObject.getString(StackData.HR_REDUCESTOTAL);
			  reducesCompleted=jobObject.getString(StackData.HR_REDUCESCOMPLETED);			 
			  String fiUrl = split(url);
			  String seHttpUrl = fiUrl+"/"+id +"/"+StackData.HR_CONF;
			  try{
				  String res =	conn(seHttpUrl);
				  JSONObject  result2 =JSON.parseObject(res);	
				  JSONObject jo = result2.getJSONObject(StackData.HR_CONF);	 
				  JSONArray ja = jo.getJSONArray(StackData.HR_PROPERTY);		  
				  for (int j = 0; j < ja.size(); j++){
					  JSONObject jsb = (JSONObject)ja.get(j);
					  if(StackData.HR_HIVE_QUERY_STRING.equals(jsb.getString(StackData.HR_NAME))) {
						  name=delGroup(jsb.getString(StackData.HR_VALUE)).toLowerCase();
						  if(!name.equals(null)&&(name.contains("from"))){
							  map.put(id, delectFanDanyin(name));
						  }			 			  
						  break;
					  }
				  }		
				  if(StackData.HR_SUCCEEDED.equals(jobObject.getString(StackData.HR_STATE))&&!jobObject.getString(StackData.HR_STATE).equals(null)){
					  state =jobObject.getString(StackData.HR_STATE);
					  JobHistory job = new JobHistory(submitTime, startTime, finishTime,id, name, queue, user, state, mapsTotal, mapsCompleted, reducesTotal, reducesCompleted);				  				  		 
					  if(list.size()==0){
						  jdbc.insertJobHistory(job);
						  continue;
					  }					  
					  for(int k=0;k<list.size();k++){	
						  if(list.get(k).getId().equals(job.getId())){						
							  num =k;
							  break;
						  }
					  }		  				  
					  if(num==-1){
						  jdbc.insertJobHistory(job);
					  }	
				  } 		    
			  }catch( Exception e){		
				  continue;		  
			  }
		  	}
		return map;   	 	  
	}
	/**
	 * 解析sql的血缘关系,并存入mysql表中
	 * @param map
	 * @throws SemanticException
	 * @throws org.apache.hadoop.hive.ql.parse.ParseException
	 * @throws ParseException
	 */
	public static void parse(HashMap<String,String> map) throws SemanticException, org.apache.hadoop.hive.ql.parse.ParseException, ParseException{	
		 LinearParse lep = new LinearParse();		 
		 Linear line = null;	
		 TreeSet outPutTables = null;
		 TreeSet inPutTables = null;
		 String str = null;
		 String strin = null;
		 Iterator ite = null;
		 Iterator it = null;
		 JDBCUtils jdbc = new JDBCUtils();
		 ArrayList<Linear> list = null;
				
		for (Entry<String, String> entry : map.entrySet()) {
			String key =	entry.getKey();
			String value =	entry.getValue();
			lep.getLineageInfo(value);
			outPutTables =lep.getOutputTableList();
			inPutTables =lep.getInputTableList();
			  ite=outPutTables.iterator();
			  it=inPutTables.iterator();
			  while(ite.hasNext()){
				   str=replaceTableName((String)ite.next());		  		  
				  while(it.hasNext()){
					   strin=replaceTableName((String)it.next());
					   line = new Linear(null, HttpResponse.stamToDate(String.valueOf(System.currentTimeMillis())), key, strin, str);  				   
					   list = jdbc.getAll();
					   if(list.size()!=0){
						   compare(list,line);				 
					   }else{
						   jdbc.insertJobHistory_Linear(line);
					   }		 			   
				  } 
			  }		
		}
	}
	
	public static void compare(ArrayList<Linear> list,Linear line) throws ParseException {			
		JDBCUtils jdbc = new JDBCUtils();
			boolean flag = false;
			String str = "";
			String linejobids =line.getSourcejobids();
			StringBuffer sb = null;
			int num =-1;
			int arr =-1;
			//先找到有没有line这个对象
			for(int i=0;i<list.size();i++){		
				Linear lin = list.get(i);				
				if(lin.getInputTable().equals(line.getInputTable())&&lin.getOutputTable().equals(line.getOutputTable())&&!lin.getSourcejobids().contains(line.getSourcejobids())){					
					sb =new StringBuffer(lin.getSourcejobids());
					str =sb.append(",").append(line.getSourcejobids()).toString();						
					num = i;				
					break;
				}else if (lin.getInputTable().equals(line.getInputTable())&&lin.getOutputTable().equals(line.getOutputTable())&&lin.getSourcejobids().contains(line.getSourcejobids())){
					arr =i;
					break;
				}
			}
			//如果有不包含则update
			if(num!=-1){
				String sql ="update jobhistory_datablood set sourcejobids ='"+str+"',updatetime ='"+HttpResponse.stamToDate(String.valueOf(System.currentTimeMillis())) +"' WHERE input_Table = '"+ line.getInputTable()+"' AND output_Table ='" + line.getOutputTable() + "'";
				jdbc.update(sql);	
			}				
			if(num==-1&&arr==-1){
				jdbc.insertJobHistory_Linear(line);
			}
 }
	/**
	 * 连接url，得到json格式的结果
	 * @param url
	 * @return
	 */
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
	/**
	 * 拆分传进来的url,取？前面的部分，用做jobid+conf拼接新的url取sql语句
	 * @param srcstring
	 * @return
	 */
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
	 /**
	  * 时间戳转换为时间格式
	  * @param stam
	  * @return
	  * @throws ParseException
	  */
	 public static String stamToDate(String stam) throws ParseException{
		 SimpleDateFormat sdf = new SimpleDateFormat(StackData.HR_TIMEFORMAT);
	     Long timeLong = Long.parseLong(stam); 
	     Date date = sdf.parse(sdf.format(timeLong));
	     String dat = sdf.format(date);
	     return dat;
	}	 
	 /**
	  * 时间转换为时间戳格式
	  */
	 public static String dateToStamp(String s) throws ParseException{
	        String res;
	        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(StackData.HR_TIMEFORMAT);
	        Date date = simpleDateFormat.parse(s);
	        long ts = date.getTime();
	        res = String.valueOf(ts);
	        System.out.println(res);
	        return res;
	    }
//	 /**
//	  * 定义url
//	  * @param strs
//	  * @return
//	  * @throws ParseException
//	  */
//	 public static String defineUrl(String[] strs) throws ParseException{
//		 String str =StackData.HR_FIRURL;
//			String url =null;
//			StringBuffer sb = new StringBuffer(str);
//			if(strs.length==0){	
//				url = str;	
//			}else if(strs.length==1){
//				
//				url= sb.append("?").append("startTime="+dateToStamp(strs[0])).toString();
//			}else if(strs.length==2){
//				url=sb.append("?").append("startTime="+dateToStamp(strs[0])).append("&user="+strs[1]).toString();
//			}	
//			 return url;
//	    } 
	 /**
	  * 更换字符串中的group字段
	  * @param str
	  * @return
	  */	
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
}
