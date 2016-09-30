package org.aksw.sparqlmap.core.r2rml.jdbc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import org.aksw.sparqlmap.core.ImplementationException;
import org.aksw.sparqlmap.core.r2rml.R2RML;
import org.aksw.sparqlmap.core.translate.jdbc.DataTypeHelper;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.jena.rdf.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionWithString;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;

public class JDBCTermMap{
	
	private static Logger log = LoggerFactory.getLogger(JDBCTermMap.class); 
	
	private DataTypeHelper dth;

	
	//  for each fromItem 
	private Set<EqualsTo> joinConditions = new HashSet<EqualsTo>();
				

	public Expression termType;
	public Expression literalType;
	public Expression literalLang;
	public Expression literalValString;
	public Expression literalValNumeric;
	public Expression literalValDate;
	public Expression literalValBool;
	public Expression literalValBinary;
	
	private List<Expression> resourceColSeg = new ArrayList<Expression>(); 
	
	private LinkedHashMap<String,FromItem> alias2fromItem = new LinkedHashMap<String, FromItem>();

	
	
	private void setExpressions(List<Expression> expressions) {
		List<Expression> exprs = new ArrayList<Expression>(expressions); //clone it, so we can work with it
		
		termType = exprs.remove(0);
		literalType = exprs.remove(0);
		literalLang= exprs.remove(0);
		literalValString= exprs.remove(0);
		literalValNumeric  = exprs.remove(0);
		literalValDate = exprs.remove(0);
		literalValBool = exprs.remove(0);
		literalValBinary = exprs.remove(0);
		resourceColSeg.addAll(exprs);
				
	}

	
	public static JDBCTermMap createNullTermMap(DataTypeHelper dth){
	  JDBCTermMap tm = createTermMap(dth, Lists.newArrayList(
        dth.castInt(new NullValue()),
        dth.castString(new NullValue()),
        dth.castString(new NullValue()),
        dth.castString(new NullValue()),
        dth.castNumeric(new NullValue()),
        dth.castDate(new NullValue()),
        dth.castBool(new NullValue()),
        dth.castBin(new NullValue())
        
        ));
	  return tm;
	}

	public static  JDBCTermMap createTermMap(DataTypeHelper dataTypeHelper, Collection<Expression> expressions) {
		JDBCTermMap tm = new JDBCTermMap(dataTypeHelper);
		tm.setExpressions(new ArrayList<Expression>(expressions));
    return tm;
			
	}
	
	public JDBCTermMap(DataTypeHelper dth){
		this.dth = dth;
		termType = dth.getIntegerDefaultExpression();
		literalType = dth.getStringDefaultExpression();
		literalLang= dth.getStringDefaultExpression();
		literalValString= dth.getStringDefaultExpression();
		literalValNumeric  =dth.getNumericDefaultExpression();
		literalValDate = dth.getDateDefaultExpression();
		literalValBool = dth.getBooleanDefaultExpression();
		literalValBinary = dth.getBinaryDefaultExpression();
		
		resourceColSeg = new ArrayList<Expression>(); 
	}
	


	public List<SelectExpressionItem> getSelectExpressionItems( String colalias){
		List<Expression> exprs = new ArrayList<Expression>(this.getExpressions()); //clone it, so we can work with it
		List<SelectExpressionItem> seis = new ArrayList<SelectExpressionItem>();
		//read the header
		
		Expression typeExpr =exprs.remove(0);
		SelectExpressionItem typeSei = new SelectExpressionItem();
		typeSei.setExpression(typeExpr);
		typeSei.setAlias(colalias + JDBCColumnHelper.COL_NAME_RDFTYPE);
		
		seis.add(typeSei);
		
			
		
		//read the litypefield
		Expression litTypeExpr = exprs.remove(0);
		SelectExpressionItem litTypeSei  = new SelectExpressionItem();
		litTypeSei.setAlias(colalias + JDBCColumnHelper.COL_NAME_LITERAL_TYPE);
		litTypeSei.setExpression(litTypeExpr);
		seis.add(litTypeSei);

		//read the lang
		Expression litlangExpr = exprs.remove(0);
		SelectExpressionItem litnangSei  = new SelectExpressionItem();
		litnangSei.setAlias(colalias + JDBCColumnHelper.COL_NAME_LITERAL_LANG);
		litnangSei.setExpression(litlangExpr);
		seis.add(litnangSei);
		
	
		//add the string col
		SelectExpressionItem stringsei = new SelectExpressionItem();
		stringsei.setAlias(colalias + JDBCColumnHelper.COL_NAME_LITERAL_STRING);
		stringsei.setExpression(exprs.remove(0));
		seis.add(stringsei);
		
		//add the numeric col
		SelectExpressionItem numSei = new SelectExpressionItem();
		numSei.setAlias(colalias + JDBCColumnHelper.COL_NAME_LITERAL_NUMERIC);
		numSei.setExpression(exprs.remove(0));
		seis.add(numSei);
		
		//add the data col
		SelectExpressionItem dateSei = new SelectExpressionItem();
		dateSei.setAlias(colalias + JDBCColumnHelper.COL_NAME_LITERAL_DATE);
		dateSei.setExpression(exprs.remove(0));
		seis.add(dateSei);
		
		
		//add the bool col
		SelectExpressionItem boolsei = new SelectExpressionItem();
		boolsei.setAlias(colalias + JDBCColumnHelper.COL_NAME_LITERAL_BOOL);
		boolsei.setExpression(exprs.remove(0));
		seis.add(boolsei);
		
		
		//add the binary col
		SelectExpressionItem binsei = new SelectExpressionItem();
		binsei.setAlias(colalias + JDBCColumnHelper.COL_NAME_LITERAL_BINARY);
		binsei.setExpression(exprs.remove(0));
		seis.add(binsei);
			
		
		
		//add all the resource expressions
		int i = 0;
		for (Expression expr: exprs) {
			SelectExpressionItem resSei = new SelectExpressionItem();
			resSei.setAlias(colalias + JDBCColumnHelper.COL_NAME_RESOURCE_COL_SEGMENT + i++);
			resSei.setExpression(expr);
			seis.add(resSei);
		}
		return seis;
	}
	
	public List<FromItem> getFromItems(){
		return Collections.unmodifiableList(new ArrayList<FromItem>(this.alias2fromItem.values()));
	}
	
	/**
	 * the join conditions that need to be added to the query for multiple from items.
	 * @return
	 */
	public Set<EqualsTo> getFromJoins(){
		return joinConditions;
		
	}
	
	
	public Set<EqualsTo> getJoinConditions() {
    return joinConditions;
  }
	
	protected DataTypeHelper getDataTypeHelper(){
		return dth;
	}
	
	public String toString(){
		StringBuffer out = new StringBuffer();
		out.append("||");
		for(Expression exp: getValueExpressions() ){
			if(! (DataTypeHelper.uncast(exp) instanceof NullValue)){
				out.append(DataTypeHelper.uncast( exp).toString());
				out.append("|");
			}
			
		}
		out.append("|");
		
		return out.toString();
	}
	

	
	
	protected Expression cloneExpression(Expression origExpr,String suffix){
		if(origExpr instanceof net.sf.jsqlparser.schema.Column){
			Column origColumn = (Column) origExpr;
			Column copyCol = new Column();
			copyCol.setColumnName(origColumn.getColumnName());
			Table copyTable = new Table(origColumn.getTable().getSchemaName(), origColumn.getTable().getName());
			copyTable.setAlias(origColumn.getTable().getAlias() + suffix);
			copyCol.setTable(copyTable);
			return copyCol;
		}else if(origExpr instanceof CastExpression){
			CastExpression cast = (CastExpression) origExpr;
			
			CastExpression clone = new CastExpression(cloneExpression(cast.getCastedExpression(),suffix), cast.getTypeName()); 
			
			return clone; 
		}else if(origExpr instanceof Function){
			Function origFunction = (Function) origExpr;
			Function clonedFunction = new Function();
			clonedFunction.setAllColumns(origFunction.isAllColumns());
			clonedFunction.setEscaped(origFunction.isEscaped());
			clonedFunction.setName(origFunction.getName());
			List<Expression> cloneExprList = new ArrayList<Expression>();
			if(origFunction.getParameters()!=null){
			for(Object expObj : origFunction.getParameters().getExpressions()){
				cloneExprList.add(cloneExpression((Expression) expObj, suffix));
			}
			}
			clonedFunction.setParameters(new ExpressionList(cloneExprList));
				
			return clonedFunction;
		}else if(origExpr instanceof CastExpression){
			CastExpression origCastExpression = (CastExpression) origExpr;
			CastExpression clonedCastExpression = new CastExpression(cloneExpression(origCastExpression.getCastedExpression(),suffix), origCastExpression.getTypeName());
			return clonedCastExpression;
			
		}else if (origExpr instanceof ExpressionWithString){
			ExpressionWithString orig = (ExpressionWithString) origExpr;
			ExpressionWithString clone = new ExpressionWithString(cloneExpression(orig.getExpression(),suffix),orig.getString());
			return clone;
		}else {
			return origExpr;
		}
	}
	
	
	
	
	
	public JDBCTermMap clone(String suffix) {
		
		JDBCTermMap clone = new JDBCTermMap(this.dth);
		clone.termType = cloneExpression(termType, suffix);
		clone.literalLang = cloneExpression(literalLang, suffix);
		clone.literalType = cloneExpression(literalType, suffix);
		clone.literalValBinary  = cloneExpression(literalValBinary, suffix);
		clone.literalValBool = cloneExpression(literalValBool, suffix);
		clone.literalValDate = cloneExpression(literalValDate, suffix);
		clone.literalValNumeric = cloneExpression(literalValNumeric, suffix);
		clone.literalValString = cloneExpression(literalValString, suffix);
		
		for(Expression resourceSegment :resourceColSeg){
			clone.resourceColSeg.add(cloneExpression(resourceSegment, suffix));
		}
		
		for(FromItem fi : alias2fromItem.values()){
			FromItem ficlone = cloneFromItem(suffix,fi);
			clone.alias2fromItem.put(ficlone.getAlias(), ficlone);
		}
		
		for(EqualsTo joinCondition : joinConditions){
			clone.joinConditions.add(cloneJoinCondition(suffix, joinCondition));
		}
		
		
		
	
			
		return clone;
	}

	private EqualsTo cloneJoinCondition(final String suffix,
			EqualsTo origjoinCondition) {

		EqualsTo cloneEq = new EqualsTo();
		Expression leftClone = cloneExpression(
				(((EqualsTo) origjoinCondition).getLeftExpression()), suffix);

		cloneEq.setLeftExpression(leftClone);
		Expression rightClone = cloneExpression(
				(((EqualsTo) origjoinCondition).getRightExpression()), suffix);
		cloneEq.setRightExpression(rightClone);

		return cloneEq;
	}

	private FromItem cloneFromItem(final String suffix,
			final FromItem fi) {
		
		final List<FromItem> clonedFromItems = new ArrayList<FromItem>();

			fi.accept(new FromItemVisitor() {
				
				@Override
				public void visit(SubJoin subjoin) {
					SubJoin fItem = new SubJoin();
					fItem.setAlias(subjoin.getAlias() + suffix);
					fItem.setJoin(subjoin.getJoin());
					fItem.setLeft(subjoin.getLeft());
					clonedFromItems.add(fItem);
				}
				
				@Override
				public void visit(SubSelect subSelect) {
					SubSelect fItem = new SubSelect();
					fItem.setAlias(subSelect.getAlias() + suffix);
					fItem.setSelectBody(subSelect.getSelectBody());
					clonedFromItems.add(fItem);
					
				}
				
				@Override
				public void visit(Table tableName) {
					FromItem fItem = new Table(tableName.getSchemaName(),tableName.getName());
					fItem.setAlias(tableName.getAlias() + suffix);
					clonedFromItems.add(fItem);
					
				}
			});
			
			if(clonedFromItems.size()!=1){
				throw new ImplementationException("Error cloning a term map.");
			}
			
			return clonedFromItems.get(0);

	}
	
	public void addFromItem(FromItem from){
		this.alias2fromItem.put(from.getAlias(), from);
	}
	
	
	


	public boolean isConstant() {
		for(Expression ex: getExpressions()){
			if(DataTypeHelper.uncast(ex) instanceof Column){
				return false;
			}
		}
		
		
		return true;
	}
	/**
	 * get all expressions that consitute this term map
	 * @return
	 */
	public List<Expression> getExpressions(){
		
		List<Expression> exprs = Lists.newArrayList(
				termType,
				literalType,
				literalLang,
				literalValString,
				literalValNumeric,
				literalValDate,
				literalValBool,
				literalValBinary);
		exprs.addAll(resourceColSeg);
		
		return exprs;
	}
	
	/**
	 * get all expressions that can hold the str() value of an term map
	 * @return
	 */
	public List<Expression> getValueExpressions(){
		
		List<Expression> exprs = Lists.newArrayList(
				literalValString,
				literalValNumeric,
				literalValDate,
				literalValBool,
				literalValBinary);
		exprs.addAll(resourceColSeg);
		
		return exprs;
	}
	
	
	public void setTermTyp(Resource tt){
		if(tt.equals(R2RML.IRI)){
			termType =dth.asInteger(JDBCColumnHelper.COL_VAL_TYPE_RESOURCE);
		}else if (tt.equals(R2RML.BLANKNODE)) {
			termType = dth.asInteger(JDBCColumnHelper.COL_VAL_TYPE_BLANK);
		} else if (tt.equals(R2RML.LITERAL)) {
			termType = dth.asInteger(JDBCColumnHelper.COL_VAL_TYPE_LITERAL);
		}
	}
	

	
	
	public Resource getTermTypeAsResource(){
		String tt = ((LongValue)DataTypeHelper.uncast(termType)).getStringValue();
		
		if(tt.equals(JDBCColumnHelper.COL_VAL_TYPE_RESOURCE.toString())){
			return R2RML.IRI;
		}else if (tt.equals(JDBCColumnHelper.COL_VAL_TYPE_BLANK.toString())) {
			return R2RML.BLANKNODE;
		} else{
			return R2RML.LITERAL;
		}
	}
	
	public void setLiteralDataType(String ldt){
		this.literalType = dth.asString(ldt);

	}
	
	public void setLiteralLang(Expression lang){
	  this.literalLang = dth.asString(lang);
	}
	
	public void setResourceExpression(List<Expression> resourceExpression){
	  this.resourceColSeg = resourceExpression;
	}
	
	
	public List<Expression> getResourceColSeg() {
		return resourceColSeg;
	}
	
	public Expression getLiteralValBool() {
		return literalValBool;
	}
	
	
	
	@Override
	public int hashCode() {
		HashCodeBuilder hcb = new HashCodeBuilder();
		for(Expression expr: getExpressions()){
			hcb.append(expr.toString());
		}
		
		return hcb.toHashCode();
	}
	
	
	
	@Override
	public boolean equals(Object obj) {

		   if (obj == null) { return false; }
		   if (obj == this) { return true; }
		   if (obj.getClass() != getClass()) {
		     return false;
		   }
		   JDBCTermMap otherTm = (JDBCTermMap) obj;
		   if(getExpressions().size()!=otherTm.getExpressions().size()){
			   return false;
		   }
		   
		   EqualsBuilder eqb =  new EqualsBuilder();
		                 
		   for(int i = 0; i< getExpressions().size(); i++){
			   eqb.append(getExpressions().get(i).toString(), otherTm.getExpressions().get(i).toString());
		   }
		                 
		   return eqb.isEquals();

	}


	public Expression getLiteralValBinary() {
		return literalValBinary;
	}
	
	public Expression getLiteralValDate() {
		return literalValDate;
	}
	
	public Expression getLiteralValNumeric() {
		return literalValNumeric;
	}
	
	public Expression getLiteralValString() {
		return literalValString;
	}
	
	public Expression getLiteralLang() {
		return literalLang;
	}
	
	public Expression getLiteralType() {
		return literalType;
	}
	
	public Expression getTermType() {
		return termType;
	}
	
	
	public List<Expression> getLiteralVals(){
	  
	  return Arrays.asList(getLiteralValBinary(), getLiteralValBool(), getLiteralValString(), getLiteralValDate(), getLiteralValNumeric());
	}
	
	
	public List<Column> getColumns(){
	  List<Column> cols = Lists.newArrayList();
	  for(Expression expression: getExpressions()){
	    expression = DataTypeHelper.uncast(expression);
	    if(expression instanceof Column){
	      cols.add((Column) expression);
	    }
	  }	  
	  
	  return cols;
	}

	/**
	 * only true, if ALL cols are null
	 * @return
	 */
  public boolean isNullTermMap() {
    boolean result = true;
    for(Expression expr: getExpressions()){
      if(!(DataTypeHelper.uncast(expr) instanceof NullValue)){
        result = false;
        break;
      }
    }
    return result;
  }


	
	public LinkedHashMap<String, FromItem> getAlias2fromItem() {
    return alias2fromItem;
  }



	
}
