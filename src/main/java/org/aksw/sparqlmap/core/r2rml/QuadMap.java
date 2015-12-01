package org.aksw.sparqlmap.core.r2rml;

import java.util.Map;

import jersey.repackaged.com.google.common.collect.Maps;
import util.QuadPosition;

public class QuadMap {


  
  private String triplesMapUri;
  private TermMap subject;
  private TermMap predicate;
  private TermMap object;
  private TermMap graph;
  private LogicalTable logicalTable;
  

  public QuadMap() {
    super();  
  }
  
  
  public String getTriplesMapUri() {
    return triplesMapUri;
  }
  public void setTriplesMapUri(String triplesMapUri) {
    this.triplesMapUri = triplesMapUri;
  }
  public TermMap getSubject() {
    return subject;
  }
  public void setSubject(TermMap subject) {
    this.subject = subject;
  }
  public TermMap getPredicate() {
    return predicate;
  }
  public void setPredicate(TermMap predicate) {
    this.predicate = predicate;
  }
  public TermMap getObject() {
    return object;
  }
  public void setObject(TermMap object) {
    this.object = object;
  }
  public TermMap getGraph() {
    return graph;
  }
  public void setGraph(TermMap graph) {
    this.graph = graph;
  }

  

  public TermMap get(QuadPosition pos){
    switch (pos) {
    case graph:
      return getGraph();
    case subject:
      return getSubject();
    case predicate:
      return getPredicate();
    case object:
      return getObject();
    default:
      return null;
    }
    
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((graph == null) ? 0 : graph.hashCode());
    result = prime * result + ((logicalTable == null) ? 0 : logicalTable.hashCode());
    result = prime * result + ((object == null) ? 0 : object.hashCode());
    result = prime * result + ((predicate == null) ? 0 : predicate.hashCode());
    result = prime * result + ((subject == null) ? 0 : subject.hashCode());
    result = prime * result + ((triplesMapUri == null) ? 0 : triplesMapUri.hashCode());
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
    QuadMap other = (QuadMap) obj;
    if (graph == null) {
      if (other.graph != null)
        return false;
    } else if (!graph.equals(other.graph))
      return false;
    if (logicalTable == null) {
      if (other.logicalTable != null)
        return false;
    } else if (!logicalTable.equals(other.logicalTable))
      return false;
    if (object == null) {
      if (other.object != null)
        return false;
    } else if (!object.equals(other.object))
      return false;
    if (predicate == null) {
      if (other.predicate != null)
        return false;
    } else if (!predicate.equals(other.predicate))
      return false;
    if (subject == null) {
      if (other.subject != null)
        return false;
    } else if (!subject.equals(other.subject))
      return false;
    if (triplesMapUri == null) {
      if (other.triplesMapUri != null)
        return false;
    } else if (!triplesMapUri.equals(other.triplesMapUri))
      return false;
    return true;
  }
  
  
  
  
  public LogicalTable getLogicalTable() {
    return logicalTable;
  }


  public void setLogicalTable(LogicalTable logicalTable) {
    this.logicalTable = logicalTable;
  }




  public static class LogicalTable{
    
    private String tablename;
        
    private String version;
    
    private String query;
    
    

    public String getTablename() {
      return tablename;
    }

    public void setTablename(String tablename) {
      this.tablename = tablename;
    }

    public String getVersion() {
      return version;
    }

    public void setVersion(String version) {
      this.version = version;
    }

    public String getQuery() {
      return query;
    }

    public void setQuery(String query) {
      this.query = query;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((query == null) ? 0 : query.hashCode());
      result = prime * result + ((tablename == null) ? 0 : tablename.hashCode());
      result = prime * result + ((version == null) ? 0 : version.hashCode());
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
      LogicalTable other = (LogicalTable) obj;
      if (query == null) {
        if (other.query != null)
          return false;
      } else if (!query.equals(other.query))
        return false;
      if (tablename == null) {
        if (other.tablename != null)
          return false;
      } else if (!tablename.equals(other.tablename))
        return false;
      if (version == null) {
        if (other.version != null)
          return false;
      } else if (!version.equals(other.version))
        return false;
      return true;
    }
    
    
    
    
  }
  
  
  
}
