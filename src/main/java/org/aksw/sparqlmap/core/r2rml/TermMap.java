package org.aksw.sparqlmap.core.r2rml;

import java.util.Objects;

import org.aksw.sparqlmap.core.ImplementationException;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeVisitor;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
//@AllArgsConstructor
public abstract class TermMap {

  private String lang;
  private String datatypIRI;
  private String termTypeIRI;
  
  public TermMap(String lang, String datatypIRI, String termTypeIRI) {
    super();
    if(termTypeIRI==null){
      throw new ImplementationException("provide termtype");
    }
    this.lang = lang;
    this.datatypIRI = datatypIRI;
    this.termTypeIRI = termTypeIRI;
  }
  
  
  
  public boolean isIRI(){
    return termTypeIRI.equals(R2RML.IRI_STRING);
  }
  public boolean isLiteral(){
    return termTypeIRI.equals(R2RML.LITERAL_STRING);
  }
  public boolean isBlank(){
    return termTypeIRI.equals(R2RML.BLANKNODE_STRING);
  } 
  
  public abstract boolean isConstant();
  public abstract boolean isColumn();
  public abstract boolean isTemplate();
  public abstract boolean isReferencing();

  
 

  


}