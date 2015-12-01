package org.aksw.sparqlmap.core;

import java.sql.SQLException;
/**
 * Indicates a problem binding the R2RML model to the underlying database.
 * 
 * @author joerg
 *
 */
public class BindingException extends RuntimeException {

  public BindingException(String string, SQLException e) {
    super(string,e);
  }

}
