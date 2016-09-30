package org.aksw.sparqlmap.core.r2rml;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.aksw.sparqlmap.core.r2rml.QuadMap.LogicalTable;
import org.apache.commons.collections.MultiMap;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;

import org.apache.jena.rdf.model.Resource;


/**
 * a represenation of a R2RML model
 * @author joerg
 *
 */
public class R2RMLMapping {
  
  // term maps indexed by the iris
  private Multimap<String,QuadMap> quadMaps = HashMultimap.create();
  
  
  public Multimap<String,QuadMap> getQuadMaps() {
    return quadMaps;
  }
  
  public void addQuadMap(QuadMap quadMap) {
    this.quadMaps.put(quadMap.getTriplesMapUri(), quadMap);
  }

  public void addQuadMaps(Collection<QuadMap> load) {
   for(QuadMap qm: load){
     addQuadMap(qm);
   }
    
  }
  public void setQuadMaps(Multimap<String, QuadMap> quadMaps) {
    this.quadMaps = quadMaps;
  }

}
