package org.aksw.sparqlmap.core;

/**
 * Indicates an invalid mapping
 * 
 * @author joerg
 * 
 */
public class MappingException extends RuntimeException {
  
  /**
   * Constructs a new Exception. 
   * 
   * @param string the reason for the failure
   */
  public MappingException(String string) {
    super(string);
  }

}
