package org.aksw.sparqlmap.core.db.impl;

import org.aksw.sparqlmap.core.db.Connector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OracleConnector extends Connector  {
	
	public static final String ORACLE_DB_NAME = "Oracle";
	
	
	private static Logger log = LoggerFactory.getLogger(OracleConnector.class);

	
	@Override
	public String getDBName() {
		return ORACLE_DB_NAME;
	}


	@Override
	public String getDriverVersion() {
		// TODO Auto-generated method stub
		return null;
	}
	
	
	
}
