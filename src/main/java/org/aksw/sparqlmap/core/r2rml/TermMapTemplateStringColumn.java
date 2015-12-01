package org.aksw.sparqlmap.core.r2rml;

public class TermMapTemplateStringColumn {
  

    private String column;
    private String string;

    public String getColumn() {
      return column;
    }

    public void setColumn(String column) {
      this.column = column;
    }

    public String getString() {
      return string;
    }

    public void setString(String string) {
      this.string = string;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((column == null) ? 0 : column.hashCode());
      result = prime * result + ((string == null) ? 0 : string.hashCode());
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
      TermMapTemplateStringColumn other = (TermMapTemplateStringColumn) obj;
      if (column == null) {
        if (other.column != null)
          return false;
      } else if (!column.equals(other.column))
        return false;
      if (string == null) {
        if (other.string != null)
          return false;
      } else if (!string.equals(other.string))
        return false;
      return true;
    }
    
    

}
