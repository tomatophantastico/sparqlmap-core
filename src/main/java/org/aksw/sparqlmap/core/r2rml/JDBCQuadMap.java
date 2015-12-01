package org.aksw.sparqlmap.core.r2rml;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import org.aksw.sparqlmap.core.mapper.compatibility.CompatibilityChecker;

import util.QuadPosition;
import net.sf.jsqlparser.statement.select.FromItem;
/**
 * This representation of a quad map is bound to the underlying JDBC model.
 * Therefore it is bound to a live database.
 * @author joerg
 *
 */
public class JDBCQuadMap implements QuadMapCompatible {
		
	private String uri;
	private JDBCTermMap subject;
	private JDBCTermMap predicate;
	private JDBCTermMap object;
	private JDBCTermMap graph;
	private QuadMap boundFrom;
	
	
  public String getUri() {
    return uri;
  }
  public void setUri(String uri) {
    this.uri = uri;
  }

  public JDBCTermMap getSubject() {
    return subject;
  }
  public void setSubject(JDBCTermMap subject) {
    this.subject = subject;
  }
  public JDBCTermMap getPredicate() {
    return predicate;
  }
  public void setPredicate(JDBCTermMap predicate) {
    this.predicate = predicate;
  }
  public JDBCTermMap getObject() {
    return object;
  }
  public void setObject(JDBCTermMap object) {
    this.object = object;
  }
  public JDBCTermMap getGraph() {
    return graph;
  }
  public void setGraph(JDBCTermMap graph) {
    this.graph = graph;
  }
  public QuadMap getBoundFrom() {
    return boundFrom;
  }
  public void setBoundFrom(QuadMap boundFrom) {
    this.boundFrom = boundFrom;
  }
  
  @Deprecated
  public JDBCQuadMap getShallowCopy() {
   
    return this;
  }
  
  public JDBCTermMap get(QuadPosition qpos) {
    switch (qpos) {
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
  
  
  /* (non-Javadoc)
   * @see org.aksw.sparqlmap.core.r2rml.QuadCompatible#getCompatibilityChecker(util.QuadPosition)
   */
  @Override
  public CompatibilityChecker getCompatibilityChecker(QuadPosition qpos){
    return get(qpos).getCompChecker();
  }
	
  		

	
}
