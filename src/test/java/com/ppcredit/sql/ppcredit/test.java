package com.ppcredit.sql.ppcredit;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.hadoop.hive.ql.parse.SemanticException;

import com.ppcredit.hqlparse.HttpResponse;
import com.ppcredit.hqlparse.JDBCUtils;
import com.ppcredit.hqlparse.Linear;
import com.ppcredit.hqlparse.LinearParse;

import jodd.util.StringBand;

public class test {
	public static void main(String[] args) throws SemanticException, org.apache.hadoop.hive.ql.parse.ParseException, ParseException{		
//		String[] args1 = new String[]{"2016-10-26 00:01:16"};
//		String url =Test2.defineUrl(args1);
////		String str="http://192.168.213.22:19888/ws/v1/history/mapreduce/jobs?startTime=1477411276693&user=liumiao";	
////		HashMap<String,String> map =HttpResponse.sendGet(str);
//		HashMap<String,String> map =HttpResponse.sendGet(url);
//		parse(map);	
	}
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
		for (Entry<String, String> entry : map.entrySet()) {
			String key =	entry.getKey();
			String value =	entry.getValue();
			lep.getLineageInfo(value);
			outPutTables =lep.getOutputTableList();
			inPutTables =lep.getInputTableList();
			  ite=outPutTables.iterator();
			  it=inPutTables.iterator();
			  while(ite.hasNext()){
				   str=(String)ite.next();		  		  
				  while(it.hasNext()){
					   strin=(String)it.next();
					   line = new Linear(null, HttpResponse.stamToDate(String.valueOf(System.currentTimeMillis())), key, strin, str);  
					   ArrayList<Linear> list =  jdbc.getAll();
					   System.out.println(list.size());				   
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
				String lj =line.getSourcejobids();
				StringBuffer sb =new StringBuffer(lj);
				int num =-1 ;
				for(int i=0;i<list.size();i++){		
							Linear lin = list.get(i);		
							if(lin.getInputTable().equals(line.getInputTable())&&lin.getOutputTable().equals(line.getOutputTable())){			
								sb.append(",").append(lin.getSourcejobids());	
								num = i;
							}
					}
				str=sb.toString();
				if(!str.equals(lj)&&num!=-1){
					flag = true;
				}
				if(flag){
					String sql ="update jobhistory_datablood set sourcejobids ='"+str+"',updatetime ='"+HttpResponse.stamToDate(String.valueOf(System.currentTimeMillis())) +"' WHERE input_Table = '"+ line.getInputTable()+"' AND output_Table ='" + line.getOutputTable() + "'";
					jdbc.update(sql);		
				}	
				if(!flag){
					jdbc.insertJobHistory_Linear(line);
				}
	 }
			
}
