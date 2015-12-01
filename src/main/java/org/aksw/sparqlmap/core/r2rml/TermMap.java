package org.aksw.sparqlmap.core.r2rml;

public abstract class TermMap {

  private String lang;
  private String datatypIRI;
  private String termTypeIRI;
  private QuadMap quadMap;
  
  
  public String getLang() {
    return lang;
  }

  public void setLang(String lang) {
    this.lang = lang;
  }

  public String getDatatypIRI() {
    return datatypIRI;
  }

  public void setDatatypIRI(String datatypIRI) {
    this.datatypIRI = datatypIRI;
  }

  public String getTermTypeIRI() {
    return termTypeIRI;
  }

  public void setTermTypeIRI(String termTypeIRI) {
    this.termTypeIRI = termTypeIRI;
  }
  
  
  public QuadMap getQuadMap() {
    return quadMap;
  }
  
  
  public void setQuadMap(QuadMap quadMap) {
    this.quadMap = quadMap;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((datatypIRI == null) ? 0 : datatypIRI.hashCode());
    result = prime * result + ((lang == null) ? 0 : lang.hashCode());
    result = prime * result + ((quadMap == null) ? 0 : quadMap.hashCode());
    result = prime * result + ((termTypeIRI == null) ? 0 : termTypeIRI.hashCode());
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
    TermMap other = (TermMap) obj;
    if (datatypIRI == null) {
      if (other.datatypIRI != null)
        return false;
    } else if (!datatypIRI.equals(other.datatypIRI))
      return false;
    if (lang == null) {
      if (other.lang != null)
        return false;
    } else if (!lang.equals(other.lang))
      return false;
    if (quadMap == null) {
      if (other.quadMap != null)
        return false;
    } else if (!quadMap.equals(other.quadMap))
      return false;
    if (termTypeIRI == null) {
      if (other.termTypeIRI != null)
        return false;
    } else if (!termTypeIRI.equals(other.termTypeIRI))
      return false;
    return true;
  }

  

}