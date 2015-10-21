package org.aksw.sparqlmap.querytests;

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

 

}
