package org.aksw.sparqlmap.core.exception;
/**
 * The parent exception for all Exceptions used in SparqlMap
 * 
 * @author joerg
 *
 */
public class SparqlMapException extends Exception{

  public SparqlMapException(String msg) {
    super(msg);
  }

  public SparqlMapException(String msg, Throwable e) {
   super(msg,e);
  }

}
