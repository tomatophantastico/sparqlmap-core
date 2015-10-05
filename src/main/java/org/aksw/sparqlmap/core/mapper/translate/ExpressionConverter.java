package org.aksw.sparqlmap.core.mapper.translate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.OrderByExpressionElement;

import org.aksw.sparqlmap.core.ImplementationException;
import org.aksw.sparqlmap.core.config.syntax.r2rml.ColumnHelper;
import org.aksw.sparqlmap.core.config.syntax.r2rml.TermMap;
import org.aksw.sparqlmap.core.config.syntax.r2rml.TermMapFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.collect.Lists;
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.query.SortCondition;
import com.hp.hpl.jena.sparql.algebra.op.OpOrder;
import com.hp.hpl.jena.sparql.expr.E_Add;
import com.hp.hpl.jena.sparql.expr.E_Bound;
import com.hp.hpl.jena.sparql.expr.E_Divide;
import com.hp.hpl.jena.sparql.expr.E_Equals;
import com.hp.hpl.jena.sparql.expr.E_GreaterThan;
import com.hp.hpl.jena.sparql.expr.E_GreaterThanOrEqual;
import com.hp.hpl.jena.sparql.expr.E_IsBlank;
import com.hp.hpl.jena.sparql.expr.E_IsIRI;
import com.hp.hpl.jena.sparql.expr.E_IsLiteral;
import com.hp.hpl.jena.sparql.expr.E_Lang;
import com.hp.hpl.jena.sparql.expr.E_LangMatches;
import com.hp.hpl.jena.sparql.expr.E_LessThan;
import com.hp.hpl.jena.sparql.expr.E_LessThanOrEqual;
import com.hp.hpl.jena.sparql.expr.E_LogicalAnd;
import com.hp.hpl.jena.sparql.expr.E_LogicalNot;
import com.hp.hpl.jena.sparql.expr.E_LogicalOr;
import com.hp.hpl.jena.sparql.expr.E_Multiply;
import com.hp.hpl.jena.sparql.expr.E_NotEquals;
import com.hp.hpl.jena.sparql.expr.E_OneOf;
import com.hp.hpl.jena.sparql.expr.E_SameTerm;
import com.hp.hpl.jena.sparql.expr.E_Str;
import com.hp.hpl.jena.sparql.expr.E_Subtract;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprAggregator;
import com.hp.hpl.jena.sparql.expr.ExprFunction;
import com.hp.hpl.jena.sparql.expr.ExprFunction0;
import com.hp.hpl.jena.sparql.expr.ExprFunction1;
import com.hp.hpl.jena.sparql.expr.ExprFunction2;
import com.hp.hpl.jena.sparql.expr.ExprFunctionN;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.expr.ExprVisitorBase;
import com.hp.hpl.jena.sparql.expr.ExprWalker;
import com.hp.hpl.jena.sparql.expr.NodeValue;


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
	OptimizationConfiguration optConf;
	
	@Autowired
	TermMapFactory tmf;
	
	
	/**
	 * simple implementation of the order by expression
	 * 
	 * @param opo
	 * @param var2col
	 * @return
	 */

	public List<OrderByElement> convert(OpOrder opo,
			Map<String, TermMap> var2termMap) {

		List<OrderByElement> obys = new ArrayList<OrderByElement>();
		for (SortCondition soCond : opo.getConditions()) {
			
			Expr expr = soCond.getExpression();

			if (expr instanceof ExprVar) {
				
				String var = expr.getVarName();
				TermMap tc  = var2termMap.get(var);

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
					List<Expression> subobyExprs =  asTermMap(subexpr,
							var2termMap).getExpressions();
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
	
	
	public Expression asFilter(Expr expr, Map<String, TermMap> var2termMap){
		
		TermMap tm = asTermMap(expr, var2termMap);
		return DataTypeHelper.uncast(tm.literalValBool);
		
		
		
	}
	
	
	public TermMap asTermMap(Expr expr,Map<String, TermMap> var2termMap){
		ExprToTermapVisitor ettm = new ExprToTermapVisitor(var2termMap);
		
		ExprWalker.walk(ettm, expr);
		
			
		
		return ettm.tms.pop();
		
	}
	
	
	public class ExprToTermapVisitor extends ExprVisitorBase{
		Stack<TermMap> tms=  new Stack<TermMap>();
		Map<String, TermMap> var2termMap;
	
		
		
		public ExprToTermapVisitor(Map<String, TermMap> var2termMap) {
			super();
			this.var2termMap = var2termMap;
		}


		@Override
		public void visit(ExprFunction0 func) {
			// TODO Auto-generated method stub
			super.visit(func);
		}
		
		
		@Override
		public void visit(ExprFunction1 func) {
		  		  
			if(func instanceof E_Bound){
				TermMap boundCheck = tms.pop();
				tms.push(translateIsBound(boundCheck));
				
			} else if(func instanceof E_LogicalNot){
				TermMap notCheck = tms.pop();
				Expression bool = DataTypeHelper.uncast( notCheck.getLiteralValBool());
				if(bool instanceof IsNullExpression){
					((IsNullExpression) bool).setNot(!((IsNullExpression) bool).isNot());
	
				}else{
					Parenthesis parenthesis = new Parenthesis(bool);
					parenthesis.setNot();
					
					notCheck = tmf.createBoolTermMap(parenthesis);
					
				}
				
				tms.push(notCheck);
				
			}else if(func instanceof E_Lang){
				TermMap langFunc = tms.pop();
				Expression lang = DataTypeHelper.uncast( langFunc.getLiteralLang());
				TermMap langTermMap = tmf.createStringTermMap(lang);
				tms.push(langTermMap);
				
			} else if (func instanceof E_Str){
				TermMap strParam = tms.pop();
				
				
				//create the coalesce function here
				
				List<Expression> strExpressions = new ArrayList<Expression>();

				strExpressions.add(dth.cast(DataTypeHelper.uncast(strParam.getLiteralValBinary()) , dth.getStringCastType()));
				strExpressions.add(dth.cast(DataTypeHelper.uncast(strParam.getLiteralValBool()) , dth.getStringCastType()));
				strExpressions.add(dth.cast(DataTypeHelper.uncast(strParam.getLiteralValDate()) , dth.getStringCastType()));
				strExpressions.add(dth.cast(DataTypeHelper.uncast(strParam.getLiteralValNumeric()) , dth.getStringCastType()));
				strExpressions.add(dth.cast(DataTypeHelper.uncast(strParam.getLiteralValString()) , dth.getStringCastType()));
				
								
				strExpressions.add(FilterUtil.concat(strParam.getExpressions().toArray(new Expression[0])));
				
				Expression toString = FilterUtil.coalesce(strExpressions.toArray(new Expression[0]));
				
				
				tms.push(tmf.createStringTermMap(toString));
			}else if(func instanceof E_IsBlank){
				TermMap isBlank =  tms.pop();
				EqualsTo eq = new EqualsTo();
				eq.setLeftExpression(isBlank.getTermType());
				eq.setRightExpression(new StringValue("'" +ColumnHelper.COL_VAL_TYPE_BLANK+ "'"));
				tms.push(tmf.createBoolTermMap(eq));
			}else if(func instanceof E_IsIRI){
				TermMap isIri =  tms.pop();
				EqualsTo eq = new EqualsTo();
				eq.setLeftExpression(isIri.getTermType());
				eq.setRightExpression(new StringValue("'" +ColumnHelper.COL_VAL_TYPE_RESOURCE+ "'"));
				tms.push(tmf.createBoolTermMap(eq));
			}else if(func instanceof E_IsLiteral){
				TermMap isLiteral =  tms.pop();
				EqualsTo eq = new EqualsTo();
				eq.setLeftExpression(isLiteral.getTermType());
				eq.setRightExpression(new StringValue("'" +ColumnHelper.COL_VAL_TYPE_LITERAL+ "'"));
				tms.push(tmf.createBoolTermMap(eq));
			}else{
				throw new ImplementationException("Implement Conversion for " + func.toString());
			}
			
			
				
		}
		
		private TermMap translateIsBound(TermMap tm){
			List<Expression> isNotNullExprs = new ArrayList<Expression>();
		isNotNullExprs.add(isExpressionBound(tm.getLiteralValBinary()));
		isNotNullExprs.add(isExpressionBound(tm.getLiteralValBool()));
		isNotNullExprs.add(isExpressionBound(tm.getLiteralValDate()));
		isNotNullExprs.add(isExpressionBound(tm.getLiteralValNumeric()));
		isNotNullExprs.add(isExpressionBound(tm.getLiteralValString()));
		
		for(Expression resExpr: tm.getResourceColSeg()){
			isNotNullExprs.add(isExpressionBound(resExpr));
		}
		
			List<Expression> nonConstantExpressions = new ArrayList<Expression>();
			for (Expression isNotNullExpr : isNotNullExprs) {
				if (isNotNullExpr instanceof StringExpression) {
					if (((StringExpression) isNotNullExpr).getString().equals(
							"true")) {
						return tmf.createBoolTermMap(isNotNullExpr);
					}
					// ignore it
				} else {
					nonConstantExpressions.add(isNotNullExpr);
				}

			}

			if (nonConstantExpressions.isEmpty()) {

				// all were shortcutted because they were null, so it is not
				// bound
				return tmf.createBoolTermMap(new StringExpression("false"));
			} else {
				return tmf.createBoolTermMap(FilterUtil
						.disjunct(nonConstantExpressions));
			}
		}
		
			
		
		private Expression isExpressionBound(Expression expr){
			expr = DataTypeHelper.uncast(expr);
			// date col check
			if(optConf.shortcutFilters&& expr instanceof NullValue){
				//do nothing
				return new StringExpression("false");
			}if(optConf.shortcutFilters&&( DataTypeHelper.constantValueExpressions.contains( expr.getClass()))){
			// constant value detected is therefore bound
				return new StringExpression("true");
			}else{
				IsNullExpression isNullExpr = new IsNullExpression();
				isNullExpr.setLeftExpression(expr);
				isNullExpr.setNot(true);
				return isNullExpr;
			}
		}
		
		
		
		@Override 
		public void visit(ExprFunction2 func) {
			TermMap left = tms.pop();
			TermMap right = tms.pop();
			
			if(func instanceof E_Equals){
				putXpathTestOnStack(left, right, EqualsTo.class );
			}else if(func instanceof E_SameTerm){
				putXpathTestOnStack(left, right, EqualsTo.class );
			}else if(func instanceof E_NotEquals){
				putXpathTestOnStack(left, right, NotEqualsTo.class);
			}else if(func instanceof E_LessThan){
				putXpathTestOnStack(left, right, MinorThan.class );
			}else if (func instanceof E_LessThanOrEqual){
				putXpathTestOnStack(left, right, MinorThanEquals.class);
			}else if(func instanceof E_GreaterThan){
				putXpathTestOnStack(left, right, GreaterThan.class);
			}else if(func instanceof E_GreaterThanOrEqual){
				putXpathTestOnStack(left, right, GreaterThanEquals.class);
			}else if(func instanceof E_LangMatches){
				putLanMatchesOnStack(left, right);				
			}else if(func instanceof E_Add){
				putArithmeticOnStack(left,right,Addition.class);
			}else if(func instanceof E_Subtract){
				putArithmeticOnStack(left, right, Subtraction.class);
			}else if(func instanceof E_Multiply){
				putArithmeticOnStack(left, right, Multiplication.class);
			}else if(func instanceof E_Divide){
				putArithmeticOnStack(left, right, Division.class);
			}else if(func instanceof E_LogicalAnd){
				putLogicalOnStack(left,right,AndExpression.class);
			}else if(func instanceof E_LogicalOr){
				putLogicalOnStack(left,right,OrExpression.class);
			}
			
			
			else{
				throw new ImplementationException("Expression not implemented:" + func.toString());
			}
			
		}
		
		


		private void putLogicalOnStack(TermMap left, TermMap right,
				Class<? extends BinaryExpression> logicalClass) {
			try {
				BinaryExpression logical =  logicalClass.newInstance();
				logical.setLeftExpression(DataTypeHelper.uncast(left.getLiteralValBool()));
				logical.setRightExpression(DataTypeHelper.uncast(right.getLiteralValBool()));
				
				tms.push(tmf.createBoolTermMap(logical));
			} catch (InstantiationException | IllegalAccessException e) {
				log.error("Error creating logical operator",e);
			}
			
			
		}


		private void putArithmeticOnStack(TermMap left, TermMap right,
				Class<? extends BinaryExpression> arithmeticOp) {
			try {
				BinaryExpression arithmetical  = arithmeticOp.newInstance();
				arithmetical.setLeftExpression(DataTypeHelper.uncast(right.getLiteralValNumeric()));
				
				arithmetical.setRightExpression(DataTypeHelper.uncast(left.getLiteralValNumeric()));
				
				
				
				// for division we always return decimal
				if(arithmeticOp.equals(Division.class)){
					Expression datatype = dth.cast(new StringValue("'" + XSDDatatype.XSDdecimal.getURI()+ "'"), dth.getStringCastType());
					tms.push(tmf.createNumericalTermMap(arithmetical, datatype));
				}else{
					
					//determine the datatype
					
					if(optConf.isShortcutFilters()){
						//check if we can determine the datatype of both parameters
						Expression dtLeft = DataTypeHelper.uncast(left.getLiteralType());
						Expression dtRight = DataTypeHelper.uncast(right.getLiteralType());
						if(DataTypeHelper.constantValueExpressions.contains(dtLeft.getClass())
								&& DataTypeHelper.constantValueExpressions.contains(dtRight.getClass())){
							if(dtLeft.toString().equals(dtRight.toString())){
								//the same, so we use
								tms.push(tmf.createNumericalTermMap(arithmetical, dtLeft));
							}else{
								//we just use decimal
								Expression datatype = dth.cast(new StringValue("'" + XSDDatatype.XSDdecimal.getURI()+ "'"), dth.getStringCastType());
								tms.push(tmf.createNumericalTermMap(arithmetical, datatype));
							}
							
							return;
						}
						
						
					}
					
					//it was not possible to short, so create the dynamic datatype expression
					CaseExpression datatypeCase = new CaseExpression();
					
					WhenClause datatypeEquals = new WhenClause();
					EqualsTo datatypesAreEqualwhen = new EqualsTo();
					datatypesAreEqualwhen.setLeftExpression(left.getLiteralType());
					Expression datatypesEqualThen = left.getLiteralType();
					
					datatypeEquals.setWhenExpression(datatypesAreEqualwhen);
					datatypeEquals.setThenExpression(datatypesEqualThen);
					
					
					Expression elseDataType = new StringValue("'" + XSDDatatype.XSDdecimal.getURI()+ "'");
					
					datatypeCase.setWhenClauses(Arrays.asList((Expression)datatypeEquals));
					
					datatypeCase.setElseExpression(elseDataType);
					
					tms.push(tmf.createNumericalTermMap(arithmetical, datatypeCase));
					
					
					
				}
				
				
				
			} catch (InstantiationException | IllegalAccessException e) {
				log.error("Error creating arithmetic operator",e);
			}
			
			
		}
		
		
		


		public void putLanMatchesOnStack(TermMap left, TermMap right) {
			EqualsTo eqExpr = new EqualsTo();
			//wrap into to_lowercase functions
			Function funcLeftLower = new Function();
			funcLeftLower.setName("LOWER");
			funcLeftLower.setParameters(new ExpressionList(Arrays.asList(left.getLiteralValString())));
			
			Function funcRightLower = new Function();
			funcRightLower.setName("LOWER");
			funcRightLower.setParameters(new ExpressionList(Arrays.asList(right.getLiteralValString())));
			
			eqExpr.setLeftExpression(funcLeftLower);
			eqExpr.setRightExpression(funcRightLower);
			
			tms.push(tmf.createBoolTermMap(eqExpr));
		}


		public void putXpathTestOnStack(TermMap left, TermMap right, Class<? extends BinaryExpression> test) {
	
	
					
					Expression binaryTestExpression  = filterUtil.compareTermMaps(left, right, test).getLiteralValBool();
					
					TermMap eqTermMap = tmf.createBoolTermMap(new Parenthesis(binaryTestExpression));
					tms.push(eqTermMap);
					
				
		}
		
		
		
		@Override
		public void visit(NodeValue nv) {		
			tms.push(tmf.createTermMap(nv.asNode()));
		}
		
		
		@Override
		public void visit(ExprVar nv) {
		  TermMap value = var2termMap.get( nv.asVar().getName());
		  if(value ==null){
		    value = tmf.createBoolTermMap(new StringExpression("false"));
		  }
			tms.push(value);

		}
		
		@Override
		public void visit(ExprAggregator eAgg) {
	
		 
		 this.var2termMap.getClass(); 
		 
		}
		
		
    @Override
    public void visit(ExprFunctionN func) {
      //correct the stack here
      
      List<TermMap> termMaps = Lists.newArrayList();
      
      for(int i = 0;i<func.getArgs().size();i++){
        termMaps.add(tms.pop());
      }
      
      termMaps = Lists.reverse(termMaps);
      
      
      
      if (func instanceof E_OneOf) {

        List<Expression> exprs = Lists.newArrayList();  
        TermMap leftHand = termMaps.remove(0);

        // just iterating here over the expressions to know how many items need
        // to be popped from the stack
        for (TermMap rightHand : termMaps) {
          TermMap comparison = filterUtil.compareTermMaps(leftHand, rightHand,
              EqualsTo.class);
          Parenthesis parenthesis = new Parenthesis(
              comparison.getLiteralValBool());
          exprs.add(parenthesis);
        }
        

        
        // disjunct it and warp it in parenthesis and put it on the stack.
        tms.push(tmf.createBoolTermMap(new Parenthesis(FilterUtil.disjunct(exprs))));

      } else {
        throw new ImplementationException("Implement Conversion for "
            + func.toString());
      }

    }
	
	}

}
