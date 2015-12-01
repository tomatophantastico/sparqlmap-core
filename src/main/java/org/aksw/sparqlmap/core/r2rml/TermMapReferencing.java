package org.aksw.sparqlmap.core.r2rml;

import java.util.List;

public class TermMapReferencing extends TermMap {

  private TermMap parent;

  private List<JoinOn> conditions;
  
  
  public TermMap getParent() {
    return parent;
  }
  
  public void setParent(TermMap parent) {
    this.parent = parent;
  }
  

  public List<JoinOn> getConditions() {
    return conditions;
  }

  public void setConditions(List<JoinOn> conditions) {
    this.conditions = conditions;
  }
  
  
  
  //included super.hashCode
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + super.hashCode();
    result = prime * result + ((conditions == null) ? 0 : conditions.hashCode());
    result = prime * result + ((parent == null) ? 0 : parent.hashCode());
    return result;
  }
  //included call to super.equals
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    if (! super.equals(obj)) 
      return false;
    TermMapReferencing other = (TermMapReferencing) obj;
    if (conditions == null) {
      if (other.conditions != null)
        return false;
    } else if (!conditions.equals(other.conditions))
      return false;
    if (parent == null) {
      if (other.parent != null)
        return false;
    } else if (!parent.equals(other.parent))
      return false;
    return true;
  }




  public static class JoinOn {

    private String childColumn;
    private String parentColumn;
    public String getChildColumn() {
      return childColumn;
    }
    public void setChildColumn(String childColumn) {
      this.childColumn = childColumn;
    }
    public String getParentColumn() {
      return parentColumn;
    }
    public void setParentColumn(String parentColumn) {
      this.parentColumn = parentColumn;
    }
    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((childColumn == null) ? 0 : childColumn.hashCode());
      result = prime * result + ((parentColumn == null) ? 0 : parentColumn.hashCode());
      return result;
    }
    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      JoinOn other = (JoinOn) obj;
      if (childColumn == null) {
        if (other.childColumn != null)
          return false;
      } else if (!childColumn.equals(other.childColumn))
        return false;
      if (parentColumn == null) {
        if (other.parentColumn != null)
          return false;
      } else if (!parentColumn.equals(other.parentColumn))
        return false;
      return true;
    }


  }
  
  
  
  
  
}
