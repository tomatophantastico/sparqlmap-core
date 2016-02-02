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
import net.sf.jsqlparser.expression.LongValue;
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
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBodyString;
import net.sf.jsqlparser.statement.select.SubSelect;

import org.aksw.sparqlmap.core.ImplementationException;
import org.aksw.sparqlmap.core.UntranslatableQueryException;
import org.aksw.sparqlmap.core.mapper.RightFirstWalker;
import org.aksw.sparqlmap.core.r2rml.JDBCColumnHelper;
import org.aksw.sparqlmap.core.r2rml.JDBCTermMap;

import com.google.common.collect.Lists;
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.sparql.expr.E_Add;
import com.hp.hpl.jena.sparql.expr.E_Bound;
import com.hp.hpl.jena.sparql.expr.E_Divide;
import com.hp.hpl.jena.sparql.expr.E_Equals;
import com.hp.hpl.jena.sparql.expr.E_Exists;
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
import com.hp.hpl.jena.sparql.expr.E_NotExists;
import com.hp.hpl.jena.sparql.expr.E_OneOf;
import com.hp.hpl.jena.sparql.expr.E_Regex;
import com.hp.hpl.jena.sparql.expr.E_SameTerm;
import com.hp.hpl.jena.sparql.expr.E_Str;
import com.hp.hpl.jena.sparql.expr.E_StrContains;
import com.hp.hpl.jena.sparql.expr.E_StrEndsWith;
import com.hp.hpl.jena.sparql.expr.E_StrStartsWith;
import com.hp.hpl.jena.sparql.expr.E_Subtract;
import com.hp.hpl.jena.sparql.expr.ExprAggregator;
import com.hp.hpl.jena.sparql.expr.ExprFunction0;
import com.hp.hpl.jena.sparql.expr.ExprFunction1;
import com.hp.hpl.jena.sparql.expr.ExprFunction2;
import com.hp.hpl.jena.sparql.expr.ExprFunctionN;
import com.hp.hpl.jena.sparql.expr.ExprFunctionOp;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.expr.ExprVisitorBase;
import com.hp.hpl.jena.sparql.expr.NodeValue;
import com.hp.hpl.jena.vocabulary.RDFS;

public class ExprToTermapVisitor extends ExprVisitorBase{

    private final ExpressionConverter expressionConverter;
    Stack<JDBCTermMap> tms=  new Stack<JDBCTermMap>();
		Map<String, JDBCTermMap> var2termMap;
		
		ExpressionHelper exHelper;
    private QueryBuilderVisitor qbv;
	
		
		
		public ExprToTermapVisitor(ExpressionConverter expressionConverter, Map<String, JDBCTermMap> var2termMap, QueryBuilderVisitor qbv) {
			super();
      this.expressionConverter = expressionConverter;
			this.var2termMap = var2termMap;
			this.qbv = qbv;
			this.exHelper = new ExpressionHelper(this.expressionConverter.dth);
		}


		@Override
		public void visit(ExprFunction0 func) {
			// TODO Auto-generated method stub
			super.visit(func);
		}
		
		
		@Override
		public void visit(ExprFunction1 func) {
		  		  
			if(func instanceof E_Bound){
				JDBCTermMap boundCheck = tms.pop();
				tms.push(translateIsBound(boundCheck));
				
			} else if(func instanceof E_LogicalNot){
				JDBCTermMap notCheck = tms.pop();
				Expression bool = DataTypeHelper.uncast( notCheck.getLiteralValBool());
				if(bool instanceof IsNullExpression){
					((IsNullExpression) bool).setNot(!((IsNullExpression) bool).isNot());
	
				}else{
					Parenthesis parenthesis = new Parenthesis(bool);
					parenthesis.setNot();
					
					notCheck = this.expressionConverter.tmf.createBoolTermMap(parenthesis);
					
				}
				
				tms.push(notCheck);
				
			}else if(func instanceof E_Lang){
				JDBCTermMap langFunc = tms.pop();
				Expression lang = DataTypeHelper.uncast( langFunc.getLiteralLang());
				JDBCTermMap langTermMap = this.expressionConverter.tmf.createStringTermMap(lang);
				tms.push(langTermMap);
				
			} else if (func instanceof E_Str){
				JDBCTermMap strParam = tms.pop();
				Expression toString;
				//check if we can determin the term type
				Expression termType = DataTypeHelper.uncast(strParam.getTermType());
				if(termType instanceof LongValue){
				  int type =  (int) (((LongValue) termType).getValue());
				  if(type == 0 || type ==1 ){
				    toString = FilterUtil.concat(strParam.getResourceColSeg().toArray(new Expression[0]));
				  }else{
				    // select the correct literal type col
				    String datatype = ((StringValue)DataTypeHelper.uncast(strParam.getLiteralType())).getValue();
				    if(datatype.equals(XSDDatatype.XSDinteger.getURI())
				       || datatype.equals(XSDDatatype.XSDdecimal.getURI())
				       || datatype.equals(XSDDatatype.XSDdouble.getURI())
               || datatype.equals(XSDDatatype.XSDfloat.getURI())){
				      toString = this.expressionConverter.dth.cast(strParam.getLiteralValNumeric(), this.expressionConverter.dth.getStringCastType()) ;
				    } if(datatype.equals(XSDDatatype.XSDdateTime.getURI())){
              toString = this.expressionConverter.dth.cast(strParam.getLiteralValDate(), this.expressionConverter.dth.getStringCastType()) ;

				    } if(datatype.equals(XSDDatatype.XSDboolean.getURI())){
              toString = this.expressionConverter.dth.cast(strParam.getLiteralValDate(), this.expressionConverter.dth.getStringCastType()) ;

				    }
				    else{
				     toString = strParam.getLiteralValString(); 
				    }
				    
				    
				  }
				  
				}else{
				  //we write the check above in SQL
				  
				  throw new ImplementationException("str is not yet implemented");
				  
				  
				}

				
				tms.push(this.expressionConverter.tmf.createStringTermMap(toString));
			}else if(func instanceof E_IsBlank){
				JDBCTermMap isBlank =  tms.pop();
				EqualsTo eq = new EqualsTo();
				eq.setLeftExpression(isBlank.getTermType());
				eq.setRightExpression(new StringValue("'" +JDBCColumnHelper.COL_VAL_TYPE_BLANK+ "'"));
				tms.push(this.expressionConverter.tmf.createBoolTermMap(eq));
			}else if(func instanceof E_IsIRI){
				JDBCTermMap isIri =  tms.pop();
				EqualsTo eq = new EqualsTo();
				eq.setLeftExpression(isIri.getTermType());
				eq.setRightExpression(new StringValue("'" +JDBCColumnHelper.COL_VAL_TYPE_RESOURCE+ "'"));
				tms.push(this.expressionConverter.tmf.createBoolTermMap(eq));
			}else if(func instanceof E_IsLiteral){
				JDBCTermMap isLiteral =  tms.pop();
				EqualsTo eq = new EqualsTo();
				eq.setLeftExpression(isLiteral.getTermType());
				eq.setRightExpression(new StringValue("'" +JDBCColumnHelper.COL_VAL_TYPE_LITERAL+ "'"));
				tms.push(this.expressionConverter.tmf.createBoolTermMap(eq));
			}else{
				throw new ImplementationException("Implement Conversion for " + func.toString());
			}
			
			
				
		}
		
		private JDBCTermMap translateIsBound(JDBCTermMap tm){
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
						return this.expressionConverter.tmf.createBoolTermMap(isNotNullExpr);
					}
					// ignore it
				} else {
					nonConstantExpressions.add(isNotNullExpr);
				}

			}

			if (nonConstantExpressions.isEmpty()) {

				// all were shortcutted because they were null, so it is not
				// bound
				return this.expressionConverter.tmf.createBoolTermMap(new StringExpression("false"));
			} else {
				return this.expressionConverter.tmf.createBoolTermMap(FilterUtil
						.disjunct(nonConstantExpressions));
			}
		}
		
			
		
		private Expression isExpressionBound(Expression expr){
			expr = DataTypeHelper.uncast(expr);
			// date col check
			if(this.expressionConverter.optConf.shortcutFilters&& expr instanceof NullValue){
				//do nothing
				return new StringExpression("false");
			}if(this.expressionConverter.optConf.shortcutFilters&&( DataTypeHelper.constantValueExpressions.contains( expr.getClass()))){
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
			JDBCTermMap left = tms.pop();
			JDBCTermMap right = tms.pop();
			
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
			}else if(func instanceof E_StrStartsWith){
			  putContainsOnStack(left,right,false,true);
			}else if(func instanceof E_StrEndsWith){
			  putContainsOnStack(left, right, true, false);
			}else if(func instanceof E_StrContains){
			  putContainsOnStack(left, right, true, true);
			}else{
				throw new ImplementationException("Expression not implemented:" + func.toString());
			}
			
		}
		
		


		private void putContainsOnStack(JDBCTermMap left, JDBCTermMap right, boolean startWildcard, boolean endWildCard) {
		  LikeExpression like = new LikeExpression();
		  
		  Expression comparator = DataTypeHelper.uncast(left.getLiteralValString());
		  if(comparator instanceof StringValue){
		    String compValue = ((StringValue) comparator).getValue();
		    if(startWildcard){
		      compValue = "%" +compValue;
		    }
		    if(endWildCard){
		      compValue = compValue + "%";
		    }
		    comparator = new StringValue("'"+compValue+"'");
		  }else{
		    throw new UntranslatableQueryException("second paramater of string comparisons (contains,strstarts,....) needs to be a fix string, cannot translate into SQL");
		  }
		  
		  
		  
		  like.setLeftExpression(right.getLiteralValString());
		  like.setRightExpression(comparator);
		  
		 
		  Expression eval = FilterUtil.conjunct( Lists.newArrayList(like,isLiteral(right)));
		
		  tms.push(this.expressionConverter.tmf.createBoolTermMap(eval));
		  
		  
    }
		
		
		private Expression isLiteral(JDBCTermMap right){
 // add the termtype and datatype check
      
      EqualsTo isLiteral = new EqualsTo();
      isLiteral.setLeftExpression(right.getTermType());
      isLiteral.setRightExpression(new LongValue("2"));
      
      
      //and check if it is a string
      EqualsTo isStringDataType = new EqualsTo();
      isStringDataType.setLeftExpression(DataTypeHelper.uncast(right.getLiteralType()));
      isStringDataType.setRightExpression(new StringValue("'" +XSDDatatype.XSDstring.getURI()+"'"));
      
      //or literal
      EqualsTo isLiteralDataType = new EqualsTo();
      isLiteralDataType.setLeftExpression( DataTypeHelper.uncast( right.getLiteralType()));
      isLiteralDataType.setRightExpression(new StringValue("'" +RDFS.Literal.getURI()+"'"));
      
      // all together
      Expression eval = FilterUtil.conjunct(
          Lists.newArrayList(
              isLiteral,
              new Parenthesis( new OrExpression(isStringDataType, isLiteralDataType))
          ));
		  return eval;
		}


    private void putLogicalOnStack(JDBCTermMap left, JDBCTermMap right,
				Class<? extends BinaryExpression> logicalClass) {
			try {
				BinaryExpression logical =  logicalClass.newInstance();
				logical.setLeftExpression(DataTypeHelper.uncast(left.getLiteralValBool()));
				logical.setRightExpression(DataTypeHelper.uncast(right.getLiteralValBool()));
				
				tms.push(this.expressionConverter.tmf.createBoolTermMap(logical));
			} catch (InstantiationException | IllegalAccessException e) {
				ExpressionConverter.log.error("Error creating logical operator",e);
			}
			
			
		}


		private void putArithmeticOnStack(JDBCTermMap left, JDBCTermMap right,
				Class<? extends BinaryExpression> arithmeticOp) {
			try {
				BinaryExpression arithmetical  = arithmeticOp.newInstance();
				arithmetical.setLeftExpression(DataTypeHelper.uncast(right.getLiteralValNumeric()));
				
				arithmetical.setRightExpression(DataTypeHelper.uncast(left.getLiteralValNumeric()));
				
				
				
				// for division we always return decimal
				if(arithmeticOp.equals(Division.class)){
					Expression datatype = this.expressionConverter.dth.cast(new StringValue("'" + XSDDatatype.XSDdecimal.getURI()+ "'"), this.expressionConverter.dth.getStringCastType());
					tms.push(this.expressionConverter.tmf.createNumericalTermMap(arithmetical, datatype));
				}else{
					
					//determine the datatype
					
					if(this.expressionConverter.optConf.isShortcutFilters()){
						//check if we can determine the datatype of both parameters
						Expression dtLeft = DataTypeHelper.uncast(left.getLiteralType());
						Expression dtRight = DataTypeHelper.uncast(right.getLiteralType());
						if(DataTypeHelper.constantValueExpressions.contains(dtLeft.getClass())
								&& DataTypeHelper.constantValueExpressions.contains(dtRight.getClass())){
							if(dtLeft.toString().equals(dtRight.toString())){
								//the same, so we use
								tms.push(this.expressionConverter.tmf.createNumericalTermMap(arithmetical, dtLeft));
							}else{
								//we just use decimal
								Expression datatype = this.expressionConverter.dth.cast(new StringValue("'" + XSDDatatype.XSDdecimal.getURI()+ "'"), this.expressionConverter.dth.getStringCastType());
								tms.push(this.expressionConverter.tmf.createNumericalTermMap(arithmetical, datatype));
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
					
					tms.push(this.expressionConverter.tmf.createNumericalTermMap(arithmetical, datatypeCase));
					
					
					
				}
				
				
				
			} catch (InstantiationException | IllegalAccessException e) {
				ExpressionConverter.log.error("Error creating arithmetic operator",e);
			}
			
			
		}
		
		
		


		public void putLanMatchesOnStack(JDBCTermMap left, JDBCTermMap right) {
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
			
			tms.push(this.expressionConverter.tmf.createBoolTermMap(eqExpr));
		}


		public void putXpathTestOnStack(JDBCTermMap left, JDBCTermMap right, Class<? extends BinaryExpression> test) {
	
	
					
					Expression binaryTestExpression  = this.expressionConverter.filterUtil.compareTermMaps(left, right, test).getLiteralValBool();
					
					JDBCTermMap eqTermMap = this.expressionConverter.tmf.createBoolTermMap(new Parenthesis(binaryTestExpression));
					tms.push(eqTermMap);
					
				
		}
		
		
		
		@Override
		public void visit(NodeValue nv) {		
			tms.push(this.expressionConverter.tmf.createTermMap(nv.asNode()));
		}
		
		
		@Override
		public void visit(ExprVar nv) {
		  JDBCTermMap value = var2termMap.get( nv.asVar().getName());
		  if(value ==null){
		    value = this.expressionConverter.tmf.createBoolTermMap(new StringExpression("false"));
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
      
      List<JDBCTermMap> termMaps = Lists.newArrayList();
      
      for(int i = 0;i<func.getArgs().size();i++){
        termMaps.add(tms.pop());
      }
      
      termMaps = Lists.reverse(termMaps);
      
      
      
      if (func instanceof E_OneOf) {

        List<Expression> exprs = Lists.newArrayList();  
        JDBCTermMap leftHand = termMaps.remove(0);

        // just iterating here over the expressions to know how many items need
        // to be popped from the stack
        for (JDBCTermMap rightHand : termMaps) {
          JDBCTermMap comparison = this.expressionConverter.filterUtil.compareTermMaps(leftHand, rightHand,
              EqualsTo.class);
          Parenthesis parenthesis = new Parenthesis(
              comparison.getLiteralValBool());
          exprs.add(parenthesis);
        }
        

        
        // disjunct it and warp it in parenthesis and put it on the stack.
        tms.push(this.expressionConverter.tmf.createBoolTermMap(new Parenthesis(FilterUtil.disjunct(exprs))));

      } else if(func instanceof E_Regex){
        
        E_Regex regexExpr = (E_Regex) func;
        JDBCTermMap valueExpression = termMaps.remove(0);
        
        String regex = regexExpr.getArg(2).getConstant().getNode().getLiteralLexicalForm();
        String flags = null;
        if(regexExpr.getArg(3)!=null){
          flags = regexExpr.getArg(3).getConstant().getNode().getLiteralLexicalForm();
        }
       
       
        Expression regexSQLExpr = this.expressionConverter.dth.regexMatches(DataTypeHelper.uncast(valueExpression.getLiteralValString()),regex,flags);
        
        Expression eval = FilterUtil.conjunct( Lists.newArrayList(regexSQLExpr,isLiteral(valueExpression)));
        
        tms.push(this.expressionConverter.tmf.createBoolTermMap(eval));
      } else {
        throw new ImplementationException("Implement Conversion for "
            + func.toString());
      }

    }
    

    @Override
    public void visit(ExprFunctionOp op) {
      if(op instanceof E_Exists){
        ExistsExpression exists = new ExistsExpression();
       
        
        
        SubSelect subselect = new SubSelect();
        subselect.setSelectBody(qbv.selects.pop());
        subselect.setAlias("exists_1");
        exists.setRightExpression(subselect);
        
        
        
        tms.push(expressionConverter.tmf.createBoolTermMap(subselect));
        
        
        
        
      }else if (op instanceof E_NotExists){
        throw new ImplementationException("Implement Conversion for "
            + op.toString());
      }else {
        throw new ImplementationException("Implement Conversion for "
            + op.toString());
      }
      
      
      
      
   
    }
	}