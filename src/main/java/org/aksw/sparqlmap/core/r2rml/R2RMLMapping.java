package org.aksw.sparqlmap.core.r2rml;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.hp.hpl.jena.rdf.model.Resource;
import com.sun.org.apache.xalan.internal.xsltc.compiler.util.MultiHashtable;


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

  public void addQuadMaps(Set<QuadMap> load) {
   for(QuadMap qm: load){
     addQuadMap(qm);
   }
    
  }
  
  
}
