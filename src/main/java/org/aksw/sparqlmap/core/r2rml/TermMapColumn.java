package org.aksw.sparqlmap.core.r2rml;

public class TermMapColumn  extends TermMap{
  
  private String column;

  public String getColumn() {
    return column;
  }

  public void setColumn(String column) {
    this.column = column;
  }

  // included super.hashCode()
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + super.hashCode();
    result = prime * result + ((column == null) ? 0 : column.hashCode());
    return result;
  }
  //included call to super.equals
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
    TermMapColumn other = (TermMapColumn) obj;
    if (column == null) {
      if (other.column != null)
        return false;
    } else if (!column.equals(other.column))
      return false;
    return true;
  }
  
  


}
