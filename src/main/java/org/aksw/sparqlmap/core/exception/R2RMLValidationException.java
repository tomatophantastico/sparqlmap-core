package org.aksw.sparqlmap.core.exception;



/**
 * Indicates an semantically invalid Mapping.
 * 
 * 
 * @author joerg
 *
 */
public class R2RMLValidationException extends SetupException{


	public R2RMLValidationException(String msg) {
		super(msg);
	}
	
	public R2RMLValidationException(String msg,Throwable e) {
		super(msg,e);
	}

	
	
	

}
