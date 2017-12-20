package com.ppcredit.hqlparse;

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
/**
 *含有insert字段的血缘关系解析类
 * @author fanqingbo
 *
 */
public class LinearParse implements NodeProcessor {
    TreeSet inputTableList = new TreeSet();
    TreeSet OutputTableList = new TreeSet();
    public TreeSet getInputTableList() {
        return inputTableList;
    }
    public TreeSet getOutputTableList() {
        return OutputTableList;
    }
    /**
     * 实现NodeProcessor的override方法
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
     * 得到血缘关系
     * @param query
     * @throws ParseException
     * @throws SemanticException
     */
    public  void getLineageInfo(String query) throws ParseException,
            SemanticException {
        ParseDriver pd = new ParseDriver();
        ASTNode tree = pd.parse(query);
        while ((tree.getToken() == null) && (tree.getChildCount() > 0)) {
            tree = (ASTNode) tree.getChild(0);
        }
        inputTableList.clear();
        OutputTableList.clear();
        Map<Rule, NodeProcessor> rules = new LinkedHashMap<Rule, NodeProcessor>();
        Dispatcher disp = new DefaultRuleDispatcher(this, rules, null);
        GraphWalker ogw = new DefaultGraphWalker(disp);
        ArrayList topNodes = new ArrayList();
        topNodes.add(tree);
        ogw.startWalking(topNodes, null);
    }
   public static void main(String[] args) {
	   
	   LinearParse ls=new LinearParse();
	   String q ="insert overwrite table tdl.test_column partition (ds='20170820') from idl.fact_node_id_daily t1 where t1.ds = '20170820'";
	   
	
}
}