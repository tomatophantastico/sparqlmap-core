package org.aksw.sparqlmap.core.r2rml;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TermMapTemplate  extends TermMap{

  private List<TermMapTemplateStringColumn> template;

  public List<TermMapTemplateStringColumn> getTemplate() {
    return template;
  }

  public void setTemplate(List<TermMapTemplateStringColumn> template) {
    this.template = template;
  }

  //included super.hashCoder
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + super.hashCode();
    result = prime * result + ((template == null) ? 0 : template.hashCode());
    return result;
  }
  // included call to super.equals
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
    TermMapTemplate other = (TermMapTemplate) obj;
    if (template == null) {
      if (other.template != null)
        return false;
    } else if (!template.equals(other.template))
      return false;
    return true;
  }   
  
  
  
}
