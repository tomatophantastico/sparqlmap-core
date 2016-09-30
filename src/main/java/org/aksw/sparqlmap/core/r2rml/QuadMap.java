package org.aksw.sparqlmap.core.r2rml;

import java.util.List;
import java.util.Map;

import org.aksw.sparqlmap.core.util.QuadPosition;

import jersey.repackaged.com.google.common.collect.Lists;
import jersey.repackaged.com.google.common.collect.Maps;
import lombok.Builder;
import lombok.Data;
@Data
@Builder
public class QuadMap {
  
  private String triplesMapUri;
  private TermMap subject;
  private TermMap predicate;
  private TermMap object;
  private TermMap graph;
  private LogicalTable logicalTable;
  
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
  
  
  public List<TermMap> getTermMaps(){
    return Lists.newArrayList(graph,subject,predicate,object);
  }

  
  @Data
  @Builder
  public static class LogicalTable{
    
    private String tablename;
        
    private String version;
    
    private String query;
    
    

    
    
  }
  
  
  
}