package com.ppcredit.hqlparse;

import java.sql.Date;
import java.util.TreeSet;
/**
 * 定义Linear类，映射mysql表jobhistory_datablood中对应字段
 * @author fanqingbo
 *
 */
public class Linear {
	private String updateTime;
	private String insertTime;
	private String sourcejobids;
	private String inputTable;
	private String OutputTable;
	
	public Linear() {
		super();
	}
	public Linear(String insertTime, String sourcejobids, String inputTable, String outputTable) {
		super();
		this.insertTime = insertTime;
		this.sourcejobids = sourcejobids;
		this.inputTable = inputTable;
		OutputTable = outputTable;
	}
	public Linear(String updateTime, String insertTime, String sourcejobids, String inPutTable, String outPutTable) {
		super();
		this.updateTime = updateTime;
		this.insertTime = insertTime;
		this.sourcejobids = sourcejobids;
		this.inputTable = inPutTable;
		this.OutputTable = outPutTable;
	}
	public String getInputTable() {
		return inputTable;
	}
	public String getInsertTime() {
		return insertTime;
	}
	public String getOutputTable() {
		return OutputTable;
	}
	public String getSourcejobids() {
		return sourcejobids;
	}
	public String getUpdateTime() {
		return updateTime;
	}
	
	public void setInputTable(String inputTable) {
		this.inputTable = inputTable;
	}
	public void setInsertTime(String insertTime) {
		this.insertTime = insertTime;
	}
	public void setOutputTable(String outputTable) {
		OutputTable = outputTable;
	}
	public void setSourcejobids(String sourcejobids) {
		this.sourcejobids = sourcejobids;
	}
	public void setUpdateTime(String updateTime) {
		this.updateTime = updateTime;
	}
}
