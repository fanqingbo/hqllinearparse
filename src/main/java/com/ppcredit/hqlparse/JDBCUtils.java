package com.ppcredit.hqlparse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
/**
 * 定义jdbc工具类
 * @author fanqingbo
 *
 */
public class JDBCUtils {
	/**
	 * ，定义函数，连接数据库
	 * @return
	 */
	public static Connection getConn() {
	    String driver = StackData.JDBC_DRIVER;
	    String url = StackData.JDBC_URL;
	    String username = StackData.JDBC_USERNAME;
	    String password = StackData.JDBC_PASSWORD;
	    Connection conn = null;
	    try {
	        Class.forName(driver);
	        conn = (Connection) DriverManager.getConnection(url, username, password);
	    } catch (ClassNotFoundException e) {
	        e.printStackTrace();
	    } catch (SQLException e) {
	        e.printStackTrace();
	    }
	    return conn;
	}
	/**
	 * 插入jobhistory表函数
	 * @param job
	 */
	public static void insertJobHistory(JobHistory job) {
	    Connection conn = getConn();
	    int i = 0;
	    String sql = "insert into jobhistory (jobid,submitTime,startTime,finishTime,name,queue,user,state,mapsTotal,mapsCompleted,reducesTotal,reducesCompleted) values(?,?,?,?,?,?,?,?,?,?,?,?)";
	    PreparedStatement pstmt;
	    try {
	        pstmt = (PreparedStatement) conn.prepareStatement(sql);
	        pstmt.setString(1, job.getId());
	        pstmt.setString(2, job.getSubmitTime());
	        pstmt.setString(3, job.getStartTime());
	        pstmt.setString(4, job.getFinishTime());
	        pstmt.setString(5, job.getName());
	        pstmt.setString(6, job.getQueue());
	        pstmt.setString(7, job.getUser());
	        pstmt.setString(8, job.getState());
	        pstmt.setString(9, job.getMapsTotal());
	        pstmt.setString(10, job.getMapsCompleted());
	        pstmt.setString(11, job.getReducesTotal());
	        pstmt.setString(12, job.getReducesCompleted());
	        i = pstmt.executeUpdate();
	        pstmt.close();
	        conn.close();
	    } catch (SQLException e) {
	        e.printStackTrace();
	    }
	}
	/**
	 * 插入jobhistory_datablood表函数
	 * @param line
	 */
	public static void insertJobHistory_Linear(Linear line) {
	    Connection conn = getConn();
	    int i = 0;
	    String sql = "insert into jobhistory_datablood (input_Table,Output_Table,insertTime,updateTime,sourcejobids) values(?,?,?,?,?)";
	    PreparedStatement pstmt;
	    try {
	        pstmt = (PreparedStatement) conn.prepareStatement(sql);
	        pstmt.setString(1, line.getInputTable());
	        pstmt.setString(2, line.getOutputTable());
	        pstmt.setString(3, line.getInsertTime());
	        pstmt.setString(4, line.getUpdateTime());
	        pstmt.setString(5, line.getSourcejobids());  
	        i = pstmt.executeUpdate();
	        pstmt.close();
	        conn.close();
	    } catch (SQLException e) {
	        e.printStackTrace();
	    }
	}
	/**
	 * 从jobhistory_datablood表中取所有行数据的方法
	 * @return
	 */
	 public static ArrayList<Linear> getAll() {
		    Connection conn = getConn();
		    String sql = "select input_table,output_table,inserttime ,sourcejobids from jobhistory_datablood";
		    PreparedStatement pstmt;
		    ArrayList<Linear> list = new ArrayList<Linear>();
		    try {
		        pstmt = (PreparedStatement)conn.prepareStatement(sql);
		        ResultSet rs = pstmt.executeQuery();       
		        while(rs.next()) {
		           Linear lin = new Linear();
		           lin.setInputTable(rs.getString("input_table"));
		           lin.setOutputTable(rs.getString("output_table"));
		           lin.setInsertTime(rs.getString("inserttime"));       
		           lin.setSourcejobids(rs.getString("sourcejobids"));     
		           list.add(lin);
		        }
		        pstmt.close();
			    conn.close();
		    } catch (SQLException e) {
		        e.printStackTrace();
		    }
			return list;   
		}
	/**
	 * 从jobhistory表中取所有行数据的方法
	 * @return
	 */
	 public static ArrayList<JobHistory> getAllFromJobHistory() {
		    Connection conn = getConn();
		    String sql = "select jobid from jobhistory";
		    PreparedStatement pstmt;
		    ArrayList<JobHistory> list = new ArrayList<JobHistory>();  
		    JobHistory job = null;
		    try {
		        pstmt = (PreparedStatement)conn.prepareStatement(sql);
		        ResultSet rs = pstmt.executeQuery();	        
		        while(rs.next()) {
		        	job = new JobHistory(rs.getString("jobid"));		       
		           list.add(job);
		        }
		        pstmt.close();
			    conn.close();
			   
		    } catch (SQLException e) {
		        e.printStackTrace();
		    }
			return list;   
		}
	 /**
	  * 更新表函数
	  * @param sql
	  */
	 public static void update( String sql) {
		    Connection conn = getConn();
		    int i = 0;
		    PreparedStatement pstmt;
		    try {
		        pstmt = (PreparedStatement) conn.prepareStatement(sql);
		        i = pstmt.executeUpdate();	        
		        pstmt.close();
		        conn.close();
		    } catch (SQLException e) {
		        e.printStackTrace();
		    } 
	}
}
