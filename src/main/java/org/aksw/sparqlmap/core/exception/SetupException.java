package org.aksw.sparqlmap.core.exception;

public class SetupException extends SparqlMapException{

  public SetupException(String msg) {
    super(msg);
   
  }

  public SetupException(String msg, Throwable e) {
    super(msg,e);
  }

}
