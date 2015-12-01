package org.aksw.sparqlmap.core.mapper.compatibility;

import java.util.HashMap;
import java.util.Map;

import org.aksw.sparqlmap.core.mapper.translate.DataTypeHelper;
import org.aksw.sparqlmap.core.r2rml.JDBCTermMap;
/**
 * A schema aware version of the simple Compatibilty checker.
 * 
 * @author joerg
 *
 */
public class JDBCCompatibilityChecker extends SimpleCompatibilityChecker{
  private JDBCTermMap termMap;
  private DataTypeHelper dth;
  //contains the cast type of the column, that would naturally be used (e.g. an tinyint will get for postgres NUMERIC)
  private Map<String,String> colname2castType = new HashMap<String, String>(); 
  
}
