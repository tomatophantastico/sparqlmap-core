package org.aksw.sparqlmap.core.r2rml;

public class TermMapConstant  extends TermMap{
  
  private String constantLiteral;
  private String constantIRI;
  
  public String getConstantLiteral() {
    return constantLiteral;
  }
  public void setConstantLiteral(String constantLiteral) {
    this.constantLiteral = constantLiteral;
  }
  public String getConstantIRI() {
    return constantIRI;
  }
  public void setConstantIRI(String constantIRI) {
    this.constantIRI = constantIRI;
  }
  
  // included the super hashCode
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + super.hashCode();
    result = prime * result + ((constantIRI == null) ? 0 : constantIRI.hashCode());
    result = prime * result + ((constantLiteral == null) ? 0 : constantLiteral.hashCode());
    return result;
  }
  
  // included the super hashCode
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    if (! super.equals(obj)) 
      return false;
    TermMapConstant other = (TermMapConstant) obj;
    if (constantIRI == null) {
      if (other.constantIRI != null)
        return false;
    } else if (!constantIRI.equals(other.constantIRI))
      return false;
    if (constantLiteral == null) {
      if (other.constantLiteral != null)
        return false;
    } else if (!constantLiteral.equals(other.constantLiteral))
      return false;
    return true;
  }

  
  
  
  
}
