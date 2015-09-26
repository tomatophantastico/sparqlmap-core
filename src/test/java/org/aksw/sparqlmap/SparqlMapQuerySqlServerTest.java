package org.aksw.sparqlmap;

import java.io.File;

import org.aksw.sparqlmap.core.SparqlMap;

public class SparqlMapQuerySqlServerTest extends SparqlMapQueryBaseTest{

  public SparqlMapQuerySqlServerTest(String testname, String dsName,
      File mappingFile, File sqlFile, String query) {
    super(testname, dsName, mappingFile, sqlFile, query);
  }

  @Override
  public SparqlMap getSparqlMap() {
    return null;
  }

  @Override
  public boolean initDb() {
    // TODO Auto-generated method stub
    return false;
  }
  


}
