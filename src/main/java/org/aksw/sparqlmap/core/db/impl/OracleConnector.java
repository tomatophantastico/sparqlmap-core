package org.aksw.sparqlmap.core.db.impl;

import org.aksw.sparqlmap.core.db.Connector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OracleConnector extends Connector  {
	
	public static final String ORACLE_DB_NAME = "Oracle";
	public static final String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";
	
	
	private static Logger log = LoggerFactory.getLogger(OracleConnector.class);

	
	@Override
	public String getDBName() {
		return ORACLE_DB_NAME;
	}

@Override
public String getDriverClassString() {
  return ORACLE_DRIVER;
}
	
	
}
