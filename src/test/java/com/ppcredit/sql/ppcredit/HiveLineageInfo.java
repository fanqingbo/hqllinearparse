package com.ppcredit.sql.ppcredit;

import java.util.TreeSet;

import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import java.io.IOException;
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
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class HiveLineageInfo implements NodeProcessor {


		 
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
		*/
		public void getLineageInfo(String query) throws ParseException,
		SemanticException {
		 
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
//		String query = " INSERT into table mdafafj (SELECT * FROM stg.ppdai_api_log_content t1 left join (SELECT id FROM `stg.ppdai_api_log` WHERE appkey='key72243b75c58a412293265d12ae086933' AND method_name='mj.out.model.query' AND CALLSTARTTIME>='2017-3-14 15:11:04' AND CALLENDTIME<='2017-3-14 17:47:32' AND batch_date='2017-03-15' ) t2 on t1.logid = t2.id where t2.id is not null and t1.batch_date='2017-03-15'))";
//			String query ="SELECT * FROM stg.ppdai_api_log_content t1 left join (SELECT id FROM `stg.ppdai_api_log` WHERE appkey='key72243b75c58a412293265d12ae086933' AND method_name='mj.out.model.query' AND CALLSTARTTIME>='2017-3-14 15:11:04' AND CALLENDTIME<='2017-3-14 17:47:32' AND batch_date='2017-03-15') t2 on t1.logid = t2.id where t2.id is not null and t1.batch_date='2017-03-15'";
			
//			String query ="SELECT id FROM `stg.ppdai_api_log` WHERE appkey='key72243b75c58a412293265d12ae086933' AND method_name='mj.out.model.query' AND CALLSTARTTIME>='2017-3-14 15:11:04' AND CALLENDTIME<='2017-3-14 17:47:32' AND batch_date='2017-03-15'";
//			String query ="SELECT id FROM stg.ppdai_api_log WHERE appkey='key72243b75c58a412293265d12ae086933' AND method_name='mj.out.model.query' AND CALLSTARTTIME>='2017-3-14 15:11:04' AND CALLENDTIME<='2017-3-14 17:47:32' AND batch_date='2017-03-15'";
			String query = "SELECT * FROM stg.ppdai_api_log_content t1 left join (SELECT id FROM `stg.ppdai_api_log`            WHERE appkey='key72243b75c58a412293265d12ae086933'            AND method_name='mj.out.model.query'            AND CALLSTARTTIME>='2017-3-14 15:11:04'            AND CALLENDTIME<='2017-3-14 17:47:32'            AND batch_date='2017-03-15'           ) t2 on t1.logid = t2.id where t2.id is not null and t1.batch_date='2017-03-15'";
			HiveLineageInfo lep = new HiveLineageInfo();
		
		lep.getLineageInfo(delectFanDanyin(query));
		System.out.println("Input tables = " + lep.getInputTableList());
		System.out.println("Output tables = " + lep.getOutputTableList());
		}
		public static String delectFanDanyin(String str){
			String regexp = "\\`";
			String strr = str.replaceAll(regexp,"");
			return strr;	
		}
		

}
