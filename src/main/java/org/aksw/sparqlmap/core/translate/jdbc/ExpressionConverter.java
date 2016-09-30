package org.aksw.sparqlmap.core.translate.jdbc;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.aksw.sparqlmap.core.r2rml.jdbc.JDBCTermMap;
import org.aksw.sparqlmap.core.r2rml.jdbc.JDBCTermMapBinder;
import org.apache.jena.query.SortCondition;
import org.apache.jena.sparql.algebra.op.OpOrder;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprFunction;
import org.apache.jena.sparql.expr.ExprVar;
import org.apache.jena.sparql.expr.ExprWalker;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.OrderByExpressionElement;


/**
 * This class allows the conversion of SPARQL Expressions into SQL Expression
 * @author joerg
 *
 */
@Component
public class ExpressionConverter {
	
	static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ExpressionConverter.class);
	
	
	

	
	
	@Autowired
	DataTypeHelper dth;
	
	@Autowired
	FilterUtil filterUtil;
	
	@Autowired
	JDBCOptimizationConfiguration optConf;
	
	@Autowired
	JDBCTermMapBinder tmf;
	
	
	/**
	 * simple implementation of the order by expression
	 * 
	 * @param opo
	 * @param var2col
	 * @return
	 */

	public List<OrderByElement> convert(OpOrder opo,
			Map<String, JDBCTermMap> var2termMap) {

		List<OrderByElement> obys = new ArrayList<OrderByElement>();
		for (SortCondition soCond : opo.getConditions()) {
			
			Expr expr = soCond.getExpression();

			if (expr instanceof ExprVar) {
				
				String var = expr.getVarName();
				JDBCTermMap tc  = var2termMap.get(var);

				for(Expression exp :tc.getExpressions()){
					//remove cast here, as e.g. hsql cannot handle them
					exp = DataTypeHelper.uncast(exp);
					//and ignore null values
					if(exp instanceof Column){
						OrderByExpressionElement ob = new OrderByExpressionElement(exp);
						ob.setAsc(soCond.getDirection() == 1 ? false : true);
						obys.add(ob);
					}
				}

			} else if (expr instanceof ExprFunction) {
				
				List<Expr> subexprs = ((ExprFunction) expr).getArgs();
				
				for (Expr subexpr : subexprs) {
				  //we hopefully do not have any exists statemetns in the order clause
					List<Expression> subobyExprs =  asTermMap(subexpr,
							var2termMap,null).getExpressions();
					for(Expression subobyExpr: subobyExprs){
						OrderByExpressionElement ob = new OrderByExpressionElement(subobyExpr);
						obys.add(ob);
					}
					
					
				}

				
				

			} else {
				log.error("Cannot handle " + expr.toString() + " in order by");
			}
			
		}
		return obys;
	}
	
	
//	/**
//	 * convert SPARQL expressions into a SQL expression that evaluates into a boolean value;
//	 * @param exp
//	 * @param colstring2var
//	 * @param colstring2col
//	 * @return
//	 */
//	public Expression getSQLWhereExpression(Expr exp,BiMap<String, String> colstring2var,
//			Map<String, TermMap> colstring2col){
//		
//		return ColumnHelper.getLiteralBoolExpression(getSQLExpression(exp, colstring2var, colstring2col));
//		
//		
//	}
	
	
	public Expression asFilter(Expr expr, Map<String, JDBCTermMap> var2termMap, QueryBuilderVisitor queryBuilderVisitor){
		
		JDBCTermMap tm = asTermMap(expr, var2termMap, queryBuilderVisitor);
		return DataTypeHelper.uncast(tm.literalValBool);
		
		
		
	}
	
	
	public JDBCTermMap asTermMap(Expr expr,Map<String, JDBCTermMap> var2termMap, QueryBuilderVisitor queryBuilderVisitor){
		ExprToTermapVisitor ettm = new ExprToTermapVisitor(this, var2termMap,queryBuilderVisitor);
		
		ExprWalker.walk(ettm, expr);
		
			
		
		return ettm.tms.pop();
		
	}

}
