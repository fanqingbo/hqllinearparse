package com.ppcredit.hqlparse;
/**
 * 定义JobHistory类，映射mysql表jobhistory中对应字段
 * @author fanqingbo
 *
 */
public class JobHistory {
	private String submitTime;
	private String startTime;
	private String finishTime;
	private String id;
	private String name;
	private String queue;
	private String user;
	private String state;
	private String mapsTotal;
	private String mapsCompleted;
	private String reducesTotal;
	private String reducesCompleted;
	public JobHistory() {
		// TODO Auto-generated constructor stub
	}
	public JobHistory(String id) {
		super();
		this.id = id;
	}
	public JobHistory(String submitTime, String startTime, String finishTime, String id, String name, String queue,
			String user, String state, String mapsTotal, String mapsCompleted, String reducesTotal,
			String reducesCompleted) {
		super();
		this.submitTime = submitTime;
		this.startTime = startTime;
		this.finishTime = finishTime;
		this.id = id;
		this.name = name;
		this.queue = queue;
		this.user = user;
		this.state = state;
		this.mapsTotal = mapsTotal;
		this.mapsCompleted = mapsCompleted;
		this.reducesTotal = reducesTotal;
		this.reducesCompleted = reducesCompleted;
	}
	
	public String getFinishTime() {
		return finishTime;
	}
	public String getId() {
		return id;
	}
	public String getMapsCompleted() {
		return mapsCompleted;
	}
	public String getMapsTotal() {
		return mapsTotal;
	}
	public String getName() {
		return name;
	}
	public String getQueue() {
		return queue;
	}
	public String getReducesCompleted() {
		return reducesCompleted;
	}
	public String getReducesTotal() {
		return reducesTotal;
	}
	public String getStartTime() {
		return startTime;
	}
	public String getState() {
		return state;
	}
	public String getSubmitTime() {
		return submitTime;
	}
     public String getUser() {
		return user;
	}
     public void setFinishTime(String finishTime) {
		this.finishTime = finishTime;
	}
     public void setId(String id) {
		this.id = id;
	}
     public void setMapsCompleted(String mapsCompleted) {
		this.mapsCompleted = mapsCompleted;
	}
     public void setMapsTotal(String mapsTotal) {
		this.mapsTotal = mapsTotal;
	}
     public void setName(String name) {
		this.name = name;
	}
     public void setQueue(String queue) {
		this.queue = queue;
	}
     public void setReducesCompleted(String reducesCompleted) {
		this.reducesCompleted = reducesCompleted;
	}
     public void setReducesTotal(String reducesTotal) {
		this.reducesTotal = reducesTotal;
	}
     public void setStartTime(String startTime) {
		this.startTime = startTime;
	}
     public void setState(String state) {
		this.state = state;
	}
	public void setSubmitTime(String submitTime) {
		this.submitTime = submitTime;
	}
	public void setUser(String user) {
		this.user = user;
	}
    
     
}
