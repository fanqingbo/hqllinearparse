package com.ppcredit.sql.ppcredit;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import com.ppcredit.hqlparse.JDBCUtils;
import com.ppcredit.hqlparse.JobHistory;
import com.ppcredit.hqlparse.StackData;

public class Test3 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ArrayList<JobHistory> list = getAllFromJobHistory();
		JobHistory job = null;
		
		for(int i =0;i<list.size();i++){
	
			job =list.get(i);
				System.out.println(list.get(i).getId());
		}
		
	}
	
	
	public static ArrayList<JobHistory> getAllFromJobHistory() {
	    Connection conn = JDBCUtils.getConn();
	    String sql = "select jobid from jobhistory";
	    PreparedStatement pstmt;
	    List list = new ArrayList();    
	    try {
	        pstmt = (PreparedStatement)conn.prepareStatement(sql);
	        ResultSet rs = pstmt.executeQuery();
	        JobHistory job = null;
	        while(rs.next()) {
	        	job = new JobHistory();	
	        	job.setId(rs.getString("jobid"));
	           list.add(job);
	        }
	        pstmt.close();
		    conn.close();
	    } catch (SQLException e) {
	        e.printStackTrace();
	    }
		return (ArrayList<JobHistory>) list;   
	}

}
