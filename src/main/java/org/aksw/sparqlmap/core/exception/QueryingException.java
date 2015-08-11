package org.aksw.sparqlmap.core.exception;

/**
 * something went wrong processing a SPARQL query
 * 
 * @author joerg
 * 
 */
public class QueryingException extends SparqlMapException {
  

  public QueryingException(String string) {
    super(string);
  }
  
  
  public QueryingException(String string,Throwable e) {
    super(string,e);
  }

}
