package com.ppcredit.hqlparse;

import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;


import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Stack;
import java.util.TreeSet;
 
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.stringLiteralSequence_return;
import org.apache.hadoop.hive.ql.parse.ParseDriver;

import org.apache.hadoop.hive.ql.parse.SemanticException;
 
/**
* lxw的大数据田地 -- lxw1234.com
* @author lxw1234
*
*/
public class Test01 implements NodeProcessor {
 
/**
* Stores input tables in sql.
*/
TreeSet inputTableList = new TreeSet();
/**
* Stores output tables in sql.
*/
TreeSet OutputTableList = new TreeSet();
 
/**
*
* @return java.util.TreeSet
*/
public TreeSet getInputTableList() {
return inputTableList;
}
 
/**
* @return java.util.TreeSet
*/
public TreeSet getOutputTableList() {
return OutputTableList;
}
 
/**
* Implements the process method for the NodeProcessor interface.
*/
public Object process(Node nd, Stack stack, NodeProcessorCtx procCtx,
Object... nodeOutputs) throws SemanticException {
ASTNode pt = (ASTNode) nd;
 
switch (pt.getToken().getType()) {
case HiveParser.TOK_CREATETABLE:
OutputTableList.add(BaseSemanticAnalyzer.getUnescapedName((ASTNode)pt.getChild(0)));
break;
case HiveParser.TOK_TAB:
OutputTableList.add(BaseSemanticAnalyzer.getUnescapedName((ASTNode)pt.getChild(0)));
break;
 
case HiveParser.TOK_TABREF:
ASTNode tabTree = (ASTNode) pt.getChild(0);
String table_name = (tabTree.getChildCount() == 1) ?
BaseSemanticAnalyzer.getUnescapedName((ASTNode)tabTree.getChild(0)) :
BaseSemanticAnalyzer.getUnescapedName((ASTNode)tabTree.getChild(0)) + "." + tabTree.getChild(1);
inputTableList.add(table_name);
break;
}
return null;
}
 
/**
* parses given query and gets the lineage info.
*
* @param query
* @throws ParseException
 * @throws org.apache.hadoop.hive.ql.parse.ParseException 
*/
public void getLineageInfo(String query) throws ParseException,
SemanticException, org.apache.hadoop.hive.ql.parse.ParseException {
 
/*
* Get the AST tree
*/
ParseDriver pd = new ParseDriver();

ASTNode tree = pd.parse(query);
 
while ((tree.getToken() == null) && (tree.getChildCount() > 0)) {
tree = (ASTNode) tree.getChild(0);
}
 
/*
* initialize Event Processor and dispatcher.
*/
inputTableList.clear();
OutputTableList.clear();
 
// create a walker which walks the tree in a DFS manner while maintaining
// the operator stack. The dispatcher
// generates the plan from the operator tree
Map<Rule, NodeProcessor> rules = new LinkedHashMap<Rule, NodeProcessor>();
 
// The dispatcher fires the processor corresponding to the closest matching
// rule and passes the context along
Dispatcher disp = new DefaultRuleDispatcher(this, rules, null);
GraphWalker ogw = new DefaultGraphWalker(disp);
 
// Create a list of topop nodes
ArrayList topNodes = new ArrayList();
topNodes.add(tree);
ogw.startWalking(topNodes, null);
}
 
public static void main(String[] args) throws IOException, ParseException,
SemanticException {
//String query = "insert overwrite table tdl.test_column partition (ds='20170820') select hgraph_v_e_id_serialize(node),hgraph_value_serialize(label),hgraph_value_serialize(concat('{', 	          '180d_listing_cnt:',180d_listing_cnt,',' 			  '180d_max_overdue:',180d_max_overdue,',' 			  '180d_trade_amount:',180d_trade_amount,',' 			  '30d_listing_cnt:',30d_listing_cnt,',' 			  '30d_max_overdue:',30d_max_overdue,',' 			  '30d_trade_amount:',30d_trade_amount,',' 			  '90d_listing_cnt:',90d_listing_cnt,',' 			  '90d_max_overdue:',90d_max_overdue,',' 			  '90d_trade_amount:',90d_trade_amount,',' 			  'curr_max_overdue:',curr_max_overdue,',' 			  'curr_max_overdue_repay:',curr_max_overdue_repay,',' 			  'curr_max_overdue_withdraw:',curr_max_overdue_withdraw,',' 			  'last_quick_amount:',last_quick_amount,',' 			  'listing_cnt:',listing_cnt,',' 			  'max_overdue:',max_overdue,',' 			  'max_overdue_repay:',max_overdue_repay,',' 			  'max_overdue_withdraw:',max_overdue_withdraw,',' 			  'quick_cnt:',quick_cnt,',' 			  'total_trade_amount:',total_trade_amount,',' 			  'court_record:',court_record,',' 			  'blacklist_status_0:',blacklist_status_0,',' 			  'blacklist_status_1:',blacklist_status_1,',' 			  'blacklist_status_2:',blacklist_status_2,',' 			  'blacklist_status_5:',blacklist_status_5,',' 			  'blacklist_status_6:',blacklist_status_6,',' 'blacklist_status_9:',blacklist_status_9,',''}')) as hehe from idl.fact_node_id_daily t1	 where t1.ds = '20170820'";
String query ="select hgraph_v_e_id_serialize(node),hgraph_value_serialize(label),hgraph_value_serialize(concat('{','180d_listing_cnt:',180d_listing_cnt,',' 			  '180d_max_overdue:',180d_max_overdue,',' 			  '180d_trade_amount:',180d_trade_amount,',' 			  '30d_listing_cnt:',30d_listing_cnt,',' '30d_max_overdue:',30d_max_overdue,',' '30d_trade_amount:',30d_trade_amount,',' 			  '90d_listing_cnt:',90d_listing_cnt,',' '90d_max_overdue:',90d_max_overdue,',' '90d_trade_amount:',90d_trade_amount,',' 			  'curr_max_overdue:',curr_max_overdue,',' 			  'curr_max_overdue_repay:',curr_max_overdue_repay,',' 'curr_max_overdue_withdraw:',curr_max_overdue_withdraw,',' 'last_quick_amount:',last_quick_amount,',' 			  'listing_cnt:',listing_cnt,',' 'max_overdue:',max_overdue,',' 'max_overdue_repay:',max_overdue_repay,',''max_overdue_withdraw:',max_overdue_withdraw,',' 'quick_cnt:',quick_cnt,',' 'total_trade_amount:',total_trade_amount,',' 'court_record:',court_record,',' 'blacklist_status_0:',blacklist_status_0,',' 'blacklist_status_1:',blacklist_status_1,',' 'blacklist_status_2:',blacklist_status_2,',' 'blacklist_status_5:',blacklist_status_5,',' 'blacklist_status_6:',blacklist_status_6,',''blacklist_status_9:',blacklist_status_9,',' '}' )) as 211045";
Test01 lep = new Test01();
try {
	lep.getLineageInfo(arr(query));
} catch (org.apache.hadoop.hive.ql.parse.ParseException e) {
	// TODO Auto-generated catch block
	e.printStackTrace();
}
System.out.println("Input tables = " + lep.getInputTableList());
System.out.println("Output tables = " + lep.getOutputTableList());
}
public static String arr(String str){
	String strr  = String.valueOf(str).toString();
	
	return strr;
	
}
}