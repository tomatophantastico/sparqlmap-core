package org.aksw.sparqlmap.core.mapper.finder;

import static org.aksw.sparqlmap.core.util.JenaHelper.getField;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.aksw.sparqlmap.core.mapper.compatibility.CompatibilityChecker;
import org.aksw.sparqlmap.core.r2rml.QuadMap;
import org.aksw.sparqlmap.core.util.QuadPosition;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.expr.Expr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import jersey.repackaged.com.google.common.collect.Lists;
import jersey.repackaged.com.google.common.collect.Maps;
import jersey.repackaged.com.google.common.collect.Sets;

/**
 * Provides a fluent api for performing operations on triple bindings is a wrapper around the quad bindings
 * 
 * @author joerg
 * 
 */
public class MappingBinding {

	private Multimap<Quad, QuadMap> bindingMap = HashMultimap.create();
	private Map<Quad, Map<String, Collection<Expr>>> quads2variables2expressions;
  private Collection<QuadMap> quadMaps;
  
  private CompatibilityChecker cchecker = new CompatibilityChecker();

  private static Logger log  = LoggerFactory.getLogger(MappingBinding.class);

	public MappingBinding(Map<Quad, Map<String, Collection<Expr>>> quads2variables2expressions,Collection<QuadMap> quadMaps) {
    super();
    this.quads2variables2expressions = quads2variables2expressions;
    this.quadMaps = quadMaps;
  }


  @Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Triple Bindings are: \n");
		Set<Quad> quads = this.bindingMap.keySet();
		for (Quad quad: quads) {
			sb.append("* " + quad.toString() + "\n");
			for (QuadMap tm : this.bindingMap.get(quad)) {
				sb.append("    QuadMap: " + tm.toString() + "\n");
				
			}
		}
		if(isEmpty()){
		  sb.append("<empty>");
		}
		
		return sb.toString();
	}
	
	
	public Multimap<Quad, QuadMap> getBindingMap() {
		return bindingMap;
	}
	
	/**
	 * indicates, if at least one triple pattern of the query could have been bound to a triples map.
	 * 
	 * @return
	 */
	public boolean isEmpty(){
	  boolean isEmpty = true;
	  
	  for(Quad quad: bindingMap.keySet()){
	    if(!bindingMap.get(quad).isEmpty()){
	      isEmpty = false;
	      break;
	    }
	  }
	  
	  return isEmpty;
	}
	
	
	
	public void init(Quad quad){
	  
	  if(!bindingMap.containsKey(quad)){
	    
	  
  
    //then check them for compatibility
    Map<String,Collection<Expr>> var2exps = quads2variables2expressions.get(quad);
    String gname = quad.getGraph().getName();
    String sname = quad.getSubject().getName();
    String pname =  quad.getPredicate().getName();
    String oname =  quad.getObject().getName();
    Collection<Expr> sxprs =  var2exps.get(sname);
    Collection<Expr> pxprs = var2exps.get(pname);
    Collection<Expr> oxprs = var2exps.get(oname);
    Collection<Expr> gxprs = var2exps.get(gname);
  
    // iterate over the subjects and remove them if they are not
    // compatible
    for (QuadMap quadmap: quadMaps) {
      boolean allCompatible = true;
      
      if(!(
             cchecker.isCompatible(quadmap.get(QuadPosition.graph),gname,gxprs)
          && cchecker.isCompatible(quadmap.get(QuadPosition.subject),sname,sxprs)
          && cchecker.isCompatible(quadmap.get(QuadPosition.predicate),pname,pxprs)
          && cchecker.isCompatible(quadmap.get(QuadPosition.object),oname,oxprs)
          )){
        allCompatible = false;
        continue;
        }
      if(allCompatible){
        bindingMap.put(quad, quadmap);
      }
 
      }
	  }
	}
	
	
	
	/**
   * merges the bindings. performs the join 
   */
  public boolean mergeBinding(Map<Quad, Collection<QuadMap>> binding1,
      Map<Quad, Collection<QuadMap>> binding2) {
    

    boolean wasmerged = false;

    // <PO> toBeRemoved = new ArrayList<TripleMap.PO>();
    boolean wasmergedthisrun = false;
    do {
      wasmergedthisrun = false;
      for (Quad quad1 : new HashSet<Quad>(binding1.keySet())) {
        for (Quad quad2 : binding2.keySet()) {
          if (!(quad1 == quad2)) {
            for (QuadPosition f1 : QuadPosition.values()) {
              for (QuadPosition f2 : QuadPosition.values()) {
          
                Node n1 = getField(quad1, f1);
                Node n2 = getField(quad2, f2);
                Collection<QuadMap> triplemaps1 = binding1
                    .get(quad1);
                Collection<QuadMap> triplemaps1_copy= null;
                if(log.isDebugEnabled()){
                  triplemaps1_copy = new HashSet<QuadMap>(binding1
                      .get(quad1));
                }
                    
                
                Collection<QuadMap> triplemaps2 = binding2
                    .get(quad2);
                if (matches(n1, n2)) {
                  wasmergedthisrun = mergeTripleMaps(f1, f2,
                      triplemaps1, triplemaps2);
                  if (wasmergedthisrun) {
                    wasmerged = true;
                  }
                  if(log.isDebugEnabled()){
                    if(wasmergedthisrun){
                      log.debug("Merged on t1: " + quad1.toString() + " x t2:" + quad2.toString());
                      log.debug("Removed the following triple maps:");
                      
                      triplemaps1_copy.removeAll(triplemaps1);
                      for (QuadMap tripleMap : triplemaps1_copy) {
                        log.debug("" +  tripleMap);
                      }
                    }else{
                      log.debug("All compatible on t1: " + quad1.toString() + " x t2:" + quad2.toString());

                    }
                    
                  }
                  
                  
                }
              }
            }
          }
          //the triple shares no variables.
          //we add the triple 
        }
      }
    } while (wasmergedthisrun);

    return wasmerged;

  }
  
  
  /**
   * modifies n1 according to doing a join on with n2
   * 
   * @return true if something was modified
   * @param n1
   * @param n2
   * @param f1
   * @param f2
   * @param triplemaps1
   * @param triplemaps2
   */
  private boolean mergeTripleMaps(QuadPosition f1, QuadPosition f2,
      Collection<QuadMap> triplemaps1, Collection<QuadMap> triplemaps2) {
    // we keep track if a modification was performed. Needed later to notify
    // the siblings.
    
    Set<QuadMap> toRemove1 = Sets.newHashSet(); 
    for(QuadMap qmc1: triplemaps1){
      
      for(QuadMap qmc2: triplemaps2){
        if(!(cchecker.isCompatible(qmc1.get(f1),qmc2.get(f2)))){
         toRemove1.add(qmc1);
        }
      }
    }
    return triplemaps1.removeAll(toRemove1);
  }
  

  
  /**
   * checks if both are variables with the same name
   * 
   * @param n1
   * @param n2
   * @return
   */
  private boolean matches(Node n1, Node n2) {
    boolean result = false;
    if (n1.isVariable() && n2.isVariable()
        && n1.getName().equals(n2.getName())) {
      result = true;
    }
    return result;
  }
  
  
  
  /**
   * Creates a List of Mapping bindings, in which every quad is quaranteed to be only associated to one quadmap.
   * This is performed by calculating the cartesian product.
   * 
   * @return
   */
  
  public Set<Map<Quad,QuadMap>> asMaps(){
    //convert the sets to Lists
  
    return asMaps(Lists.newArrayList(bindingMap.keySet()));
    
  }
  
  private Set<Map<Quad,QuadMap>> asMaps(List<Quad> qml){
    
    Set<Map<Quad,QuadMap>> result = Sets.newHashSet();
    
    if(qml.size()==1){
      
      Quad quad =  qml.get(0);
      for(QuadMap qm :  bindingMap.get(quad)){
        Map<Quad,QuadMap> binding = Maps.newHashMap();
        binding.put(quad, qm);
        result.add(binding);
      }
    }else if(qml.size()>1){
      Set<Map<Quad,QuadMap>> toMultiplies =  asMaps(qml.subList(1, qml.size()));
      Set<Map<Quad,QuadMap>> resultsNew = Sets.newHashSet();
      for(Map<Quad,QuadMap> toMultiply: toMultiplies){
        for(Map<Quad,QuadMap> res: result){
          Map<Quad,QuadMap> binding = Maps.newHashMap();
          binding.putAll(toMultiply);
          binding.putAll(res);
          resultsNew.add(binding);
        }
      }
      result = resultsNew;
    }
    
    
    
    return result;
    
    
   
  }
  
   
}