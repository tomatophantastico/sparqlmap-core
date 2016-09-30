package org.aksw.sparqlmap.core.translate.jdbc;

import java.sql.Types;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import org.aksw.sparqlmap.core.db.DBAccess;
import org.aksw.sparqlmap.core.r2rml.jdbc.JDBCColumnHelper;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.sparql.algebra.op.OpSlice;
import org.springframework.beans.factory.annotation.Autowired;

import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.StringExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.PlainSelect;

public abstract  class DataTypeHelper {
	
	
	public abstract String getDBName();
	
	static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(DataTypeHelper.class);
	
	@Autowired
	private DBAccess dbaccess;
	
	public static Set<Class> constantValueExpressions;
	
	{
		constantValueExpressions =new HashSet<Class>();
		constantValueExpressions.add(StringValue.class);
		constantValueExpressions.add(StringExpression.class);
		constantValueExpressions.add(DateValue.class);
		constantValueExpressions.add(TimestampValue.class);
		constantValueExpressions.add(TimeValue.class);
		constantValueExpressions.add(LongValue.class);
		constantValueExpressions.add(DoubleValue.class);
		constantValueExpressions.add(NullValue.class);
	}
		
	Map<String,String> suffix2datatype = new HashMap<String, String>();
	
	public DataTypeHelper() {
		suffix2datatype.put(JDBCColumnHelper.COL_NAME_LITERAL_BINARY,this.getBinaryDataType());
		suffix2datatype.put(JDBCColumnHelper.COL_NAME_LITERAL_BOOL,this.getBooleanCastType());
		suffix2datatype.put(JDBCColumnHelper.COL_NAME_LITERAL_DATE, this.getDateCastType());
		suffix2datatype.put(JDBCColumnHelper.COL_NAME_LITERAL_LANG, this.getStringCastType());
		suffix2datatype.put(JDBCColumnHelper.COL_NAME_LITERAL_NUMERIC,this.getNumericCastType());
		suffix2datatype.put(JDBCColumnHelper.COL_NAME_LITERAL_STRING, this.getStringCastType());
		suffix2datatype.put(JDBCColumnHelper.COL_NAME_LITERAL_TYPE, this.getStringCastType());
		suffix2datatype.put(JDBCColumnHelper.COL_NAME_RDFTYPE, this.getNumericCastType());
		suffix2datatype.put(JDBCColumnHelper.COL_NAME_RESOURCE_COL_SEGMENT, this.getStringCastType());
	}
	
	
	public static RDFDatatype getRDFDataType(int sdt) {
	  
		RDFDatatype datatype = null;
		
		if(sdt == Types.DECIMAL 
		    || sdt == Types.NUMERIC){
		  datatype =  XSDDatatype.XSDdecimal;
		  
		}else if(sdt== Types.BIGINT 
		    || sdt == Types.INTEGER 
		    || sdt == Types.SMALLINT){
		  datatype =  XSDDatatype.XSDinteger;
		  
		}else if( sdt == Types.FLOAT 
		    || sdt == Types.DOUBLE 
		    || sdt ==Types.REAL ){
		  datatype =  XSDDatatype.XSDdouble;
		  
		}else if(sdt == Types.VARCHAR 
		    || sdt == Types.CHAR 
		    || sdt == Types.CLOB 
		    || sdt == Types.LONGNVARCHAR 
		    || sdt == Types.LONGVARCHAR ){
			// do nothing
		  
		}else if(sdt == Types.DATE ){
		  datatype = XSDDatatype.XSDdate;
		  
		}else if(sdt == Types.TIME){
		  datatype = XSDDatatype.XSDtime;

		}else if( sdt == Types.TIMESTAMP){
		  datatype = XSDDatatype.XSDdateTime;

		}else if(sdt == Types.BOOLEAN 
		    || sdt == Types.BIT){
	    //the jsbc driver makes no differentiation between bit and boolean, so wetake them both here
		  datatype = XSDDatatype.XSDboolean;
		  
		}else if(sdt == Types.BINARY 
		    || sdt ==  Types.VARBINARY 
		    ||sdt ==  Types.BLOB 
		    || sdt == Types.LONGVARBINARY){
		  datatype = XSDDatatype.XSDhexBinary;
		  
		}else{
      log.info("encountered non-explicitly mapped sql type:" + sdt);
		}
		
		return datatype; //XSDDatatype.XSDstring;
	
	}
	
	public String getCastTypeString(int sdt){
	   String castString = null;

	  RDFDatatype dt = getRDFDataType(sdt);
	  
	  if(dt !=null){
	    castString = getCastTypeString(dt);
	  }
	  
	  
	  
		return castString;
	}
	

	
	
	
	
	public String getCastTypeString(RDFDatatype datatype){
	  String result=  null;
	  if(datatype == null ){
	    result = getStringCastType();
	  }else{
	  
  	  String dtResString = datatype.getURI();
  	  if(XSDDatatype.XSDstring.getURI().equals(dtResString)){
  	    result = getStringCastType();
      } else if(XSDDatatype.XSDdecimal.getURI().equals(dtResString)|XSDDatatype.XSDinteger.getURI().equals(dtResString) || XSDDatatype.XSDdouble.getURI().equals(dtResString)){
        result = getNumericCastType();
  		}else if(XSDDatatype.XSDdateTime.getURI().equals(dtResString)|| XSDDatatype.XSDdate.getURI().equals(dtResString) ||  XSDDatatype.XSDtime.getURI().equals(dtResString)){
  		  result = getDateCastType();
  		}else if(XSDDatatype.XSDboolean.getURI().equals(dtResString)){
  		  result = getBooleanCastType();
  		}else if(XSDDatatype.XSDhexBinary.getURI().equals(dtResString)){
  		  result = getBinaryDataType();
  		}else{
  		  result = getStringCastType();
  		}
	  }
	  return result;
	}
	
//	public Expression cast(String table, String col, String castTo) {
//		Function cast = new Function();
//		cast.setName("CAST");
//		ExpressionList exprlist = new ExpressionList();
//		exprlist.setExpressions(Arrays.asList(new CastStringExpression(table,
//				col, castTo)));
//		cast.setParameters(exprlist);
//		return cast;
//	}

	public Expression castNull(String castTo) {
		
		return new CastExpression(new NullValue(), castTo);
		
	}
	
	
	String getDataType(Expression expr){
		
		log.warn("Called getDataType. Refactor to not use direct col access");
		
		
		if(expr instanceof Column){
			String colname = ((Column) expr).getColumnName();
			String tablename = ((Column) expr).getTable().getName();
			
			//only shorten, if the table is not from a subselect
			if(!tablename.contains("subsel_")){
//				if(tablename.contains("_dupVar_")){
//					tablename=tablename.substring(0,tablename.lastIndexOf("_dupVar_"));
//				}
//				//remove the variable part
//				tablename = tablename.substring(0,tablename.lastIndexOf("_"));
				
				return this.getCastTypeString(dbaccess.getDataType(tablename, colname));
			}
			
		}
		if(expr instanceof StringValue){
			
			Scanner scanner = new Scanner(((StringValue) expr).getValue());

			if(scanner.hasNextBigDecimal()){
			  scanner.close();
				return this.getNumericCastType();
			}
			
			scanner.close();
			return this.getStringCastType();
		}

		
		if(expr instanceof CastExpression){
			return ((CastExpression) expr).getTypeName();
		}
		
		return null;
		
	}
	
	public Expression cast(Expression expr, String castTo) {
		if(castTo == null){
			return expr;
		}
		
		if(expr instanceof Column){
			
			if (needsSpecialCastForBinary()) {
				// get the datatype
				Column col = (Column) expr;
				Integer datatypeint = dbaccess.getDataType(col.getTable()
						.getAlias(), col.getColumnName());
				if (datatypeint != null && getRDFDataType(datatypeint)!=null && getRDFDataType(datatypeint).equals(XSDDatatype.XSDhexBinary)) {
					// we need to wrap the cast additionally in a substring
						
					expr = binaryCastPrep(expr);

				}
			}else if (needsSpecialCastForChar()) {
				Column col = (Column) expr;
				Integer datatypeint = dbaccess.getDataType(col.getTable()
						.getAlias(), col.getColumnName());
				if (datatypeint != null  &&  datatypeint == Types.CHAR) {
					expr = charCastPrep(expr, dbaccess.getPrecision(col.getTable()
						.getAlias(), col.getColumnName()));

				}
			}
			
			
			
			//we have special casting needs for non-varchar and binary types.
		}
		

		return new CastExpression(expr,
				castTo);
	}
	
	/**
	 * if the expressions expr is a cast, the cast expression is returned,
	 * otherwise the expr parameter is returned
	 * 
	 * @param expr
	 */
	public static Expression uncast(Expression expr) {

		if(expr instanceof CastExpression){
			expr = ((CastExpression)expr).getCastedExpression();
		}
		
		return expr;
	}
	
	public static String getCastType(Expression expr) {

		String type = null;
		if (expr instanceof CastExpression) {
			type = ((CastExpression)expr).getTypeName();
		}
		return type;
	}
	
	
	public byte[] binaryResultSetTreatment(byte[] bytes){
		return bytes;
	}
	
	
	
	public abstract String getBinaryDataType();

	public abstract String getStringCastType();
	
	public abstract String getNumericCastType();
	
	public abstract String getBooleanCastType();
	
	public abstract String getDateCastType();

	public abstract String getIntCastType();
	
	public boolean needsSpecialCastForBinary(){
	  return false;
	}
	

	
	public  Expression binaryCastPrep(Expression expr){
	  return null;
	}
	
	public abstract boolean needsSpecialCastForChar();
	
	public abstract Expression charCastPrep(Expression expr, Integer fieldlength);




	public PlainSelect slice(PlainSelect toModify, OpSlice slice) {
		Limit limit = new Limit();
		if (slice.getStart() >= 0) {
			limit.setOffset(slice.getStart());
		}
		if (slice.getLength() >= 0) {
			limit.setRowCount(slice.getLength());
		}

		toModify.setLimit(limit);
		return toModify;
	}

	public String getValidateFromQuery(String from) {
		return "SELECT * FROM " + from + " LIMIT 1";
	}

	public String getColnameQuery(String colname, String from) {
		return "SELECT " + colname + " FROM " +from+  " LIMIT 1";
	}
	
	public String getDataTypeQuery(String colname, String from) {
		return 		getColnameQuery("\""+colname+"\"", from);
	}
	
	public String getCastType(String colname){
		
		for(String suffix: this.suffix2datatype.keySet()){
			if(colname.endsWith(suffix)){
				return (suffix2datatype.get(suffix));
			}
		}
		return null;
		
		
	}
	
	public Expression asNumeric(Integer intVal){
		return cast(new LongValue(intVal.toString()), getNumericCastType());
	}
	
	public Expression asInteger(Integer intVal){
    return cast(new LongValue(intVal.toString()), getIntCastType());
  }
	
	public Expression asString(String string){
    return cast(new StringValue(string), getStringCastType());
  }
	 public Expression asString(Expression string){
	    return cast(string, getStringCastType());
	  }
	
	public Expression getStringDefaultExpression(){
	  return cast(new StringValue("''"),getStringCastType());
	}
	
	public Expression getIntegerDefaultExpression(){
	  return cast(new LongValue("0"),getIntCastType());
	}
	
	public Expression getNumericDefaultExpression(){
	  return cast(new LongValue("0"),getNumericCastType());
	}
	
	public Expression getBooleanDefaultExpression(){
    return cast(new LongValue("0"),getBooleanCastType());
  }
	
	public Expression getDateDefaultExpression(){
	  return cast(new StringValue("'0001-1-1 00:00:00.000'"),getDateCastType());
	}
	
	public Expression getBinaryDefaultExpression(){
	  return cast(new StringValue("'0x01'"),getBinaryDataType());
	}
  
	
	public Expression getDefaultValue(String castType){
	  Expression result = null;
	  
	  if(castType.equals(getStringCastType())){
	    result =  getStringDefaultExpression();
	  }else if(castType.equals(getBinaryDataType())){
	    result = getBinaryDefaultExpression();
	  }else if(castType.equals(getBooleanCastType())){
	    result = getBooleanDefaultExpression();
	  }else if(castType.equals(getDateCastType())){
	    result = getDateDefaultExpression();
	  }else if(castType.equals(getIntCastType())){
	    result = getIntegerDefaultExpression();
	  }else if(castType.equals(getNumericCastType())){
	    result = getNumericDefaultExpression();
	  }

	  return result;
	  
	  
	}
	
	
	public Expression castInt(Expression expr){
	  return cast(expr, getIntCastType());
	}
	public Expression castNumeric(Expression expr){
    return cast(expr, getNumericCastType());
  }
	public Expression castString(Expression expr){
    return cast(expr, getStringCastType());
  } 
	public Expression castBool(Expression expr){
    return cast(expr, getBooleanCastType());
  }
	public Expression castBin(Expression expr){
    return cast(expr, getBinaryDataType());
  }
	public Expression castDate(Expression expr){
    return cast(expr, getDateCastType());
  }


  public String getRowIdTemplate() {
    return null;
  }

/**
 * as there is no ansi-sql way of doing it, map here explicitly to the vendor specific versions.
 * 
 * @param literalValString
 * @param regex
 * @param flags
 * @return
 */
  public abstract Expression regexMatches(Expression literalValString, String regex, String flags);


  public static boolean isNumeric(String datatype1) {
    boolean result = false;
    if(datatype1!=null
        &&(
           datatype1.equals(XSDDatatype.XSDinteger.getURI()) 
           || datatype1.equals(XSDDatatype.XSDfloat.getURI())
           || datatype1.equals(XSDDatatype.XSDdecimal.getURI())
           || datatype1.equals(XSDDatatype.XSDdouble.getURI())
            )){
      result = true; 
    }
    return result;
  }


}
