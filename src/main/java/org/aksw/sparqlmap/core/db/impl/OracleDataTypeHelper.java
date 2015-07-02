package org.aksw.sparqlmap.core.db.impl;

import net.sf.jsqlparser.expression.Expression;

import org.aksw.sparqlmap.core.mapper.translate.DataTypeHelper;

public class OracleDataTypeHelper extends DataTypeHelper {
	
	
	public String getDBName() {
		return OracleConnector.ORACLE_DB_NAME;
	}

	@Override
	public String getStringCastType() {
		return "VARCHAR2(4000)";
	}

	@Override
	public String getNumericCastType() {
		return "NUMBER";
	}

	@Override
	public String getBooleanCastType() {
		return "CHAR";
	}

	@Override
	public String getDateCastType() {
		return "DATE";
	}

	@Override
	public String getIntCastType() {
		return "NUMBER";
	}

	@Override
	public String getBinaryDataType() {
		return "VARCHAR2(4000)";
	}

	@Override
	public boolean needsSpecialCastForBinary() {
		return false;
	}

	@Override
	public Expression binaryCastPrep(Expression expr) {
		return null;
	}

	@Override
	public boolean needsSpecialCastForChar() {
		return false;
	}

	@Override
	public Expression charCastPrep(Expression expr,Integer length) {
		return null;
	}

	@Override
	public byte[] binaryResultSetTreatment(byte[] bytes) {
		return bytes;
	}

	@Override
	public boolean hasRowIdFunction() {
		
		return false;
	}
	
	@Override
	public String getValidateFromQuery(String from) {
		return "select *\n" + 
				"  from  \n" + 
				"( select * \n" + 
				"    from " + from + " \n" + 
				"  ) \n" + 
				" where ROWNUM <= 1";
	}
	@Override
	public String getColnameQuery(String colname, String from) {
		
		return "select *\n" + 
				"  from  \n" + 
				"( select "+ colname +" \n" + 
				"    from " + from + " \n" + 
				"  ) \n" + 
				" where ROWNUM <= 1";

	}
}
