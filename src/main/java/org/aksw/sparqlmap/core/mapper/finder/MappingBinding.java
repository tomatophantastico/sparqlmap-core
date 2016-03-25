package org.aksw.sparqlmap.core.mapper.finder;

import static util.JenaHelper.getField;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import jersey.repackaged.com.google.common.collect.Sets;

import org.aksw.sparqlmap.core.r2rml.JDBCQuadMap;
import org.aksw.sparqlmap.core.r2rml.JDBCTermMap;
import org.aksw.sparqlmap.core.r2rml.BoundQuadMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import util.QuadPosition;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.sparql.expr.Expr;

/**
 * Provides a fluent api for performing operations on triple bindings is a wrapper around the quad bindings
 * 
 * @author joerg
 * 
 */
public class MappingBinding {

	private Multimap<Quad, BoundQuadMap> bindingMap = HashMultimap.create();
	private Map<Quad, Map<String, Collection<Expr>>> quads2variables2expressions;
  private Collection<BoundQuadMap> quadMaps;

  private static Logger log  = LoggerFactory.getLogger(MappingBinding.class);

	public MappingBinding(Map<Quad, Map<String, Collection<Expr>>> quads2variables2expressions,Collection<BoundQuadMap> quadMaps) {
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
			for (BoundQuadMap tm : this.bindingMap.get(quad)) {
				sb.append("    QuadMap: " + tm.toString() + "\n");
				
			}
		}
		return sb.toString();
	}
	
	
	public Multimap<Quad, BoundQuadMap> getBindingMap() {
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
    for (BoundQuadMap tripleMap: quadMaps) {
      boolean allCompatible = true;
      
      if(!(
             tripleMap.getCompatibilityChecker(QuadPosition.graph).isCompatible(gname,gxprs)
          && tripleMap.getCompatibilityChecker(QuadPosition.subject).isCompatible(sname,sxprs)
          && tripleMap.getCompatibilityChecker(QuadPosition.predicate).isCompatible(pname,pxprs)
          && tripleMap.getCompatibilityChecker(QuadPosition.object).isCompatible(oname,oxprs)
          )){
        allCompatible = false;
        continue;
        }
      if(allCompatible){
        bindingMap.put(quad, tripleMap);
      }
 
      }
	  }
	}
	
	
	
	/**
   * merges the bindings. performs the join 
   */
  public boolean mergeBinding(Map<Quad, Collection<JDBCQuadMap>> binding1,
      Map<Quad, Collection<JDBCQuadMap>> binding2) {
    

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
                Collection<JDBCQuadMap> triplemaps1 = binding1
                    .get(quad1);
                Collection<JDBCQuadMap> triplemaps1_copy= null;
                if(log.isDebugEnabled()){
                  triplemaps1_copy = new HashSet<JDBCQuadMap>(binding1
                      .get(quad1));
                }
                    
                
                Collection<JDBCQuadMap> triplemaps2 = binding2
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
                      for (BoundQuadMap tripleMap : triplemaps1_copy) {
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
      Set<BoundQuadMap> triplemaps1, Set<BoundQuadMap> triplemaps2) {
    // we keep track if a modification was performed. Needed later to notify
    // the siblings.
    boolean mergedSomething = false;
    
    Set<BoundQuadMap> toRemove1 = Sets.newHashSet(); 
    for(BoundQuadMap qmc1: triplemaps1){
      
      for(BoundQuadMap qmc2: triplemaps2){
        if(qmc1.getCompatibilityChecker(f1).isCompatible(qmc2.getCompatibilityChecker(f2))){
          
        }
        
        
      }
    }
    

    // we iterate over all triplemaps of both (join-style)
    for (BoundQuadMap triplemap1 : new HashSet<JDBCQuadMap>(triplemaps1)) {
      Set<PO> toRetain = new HashSet<JDBCQuadMap.PO>();
      for (PO po1 : new HashSet<PO>(triplemap1.getPos())) {
        for (BoundQuadMap triplemap2 : triplemaps2) {
          // we iterate over the PO, as each generates a triple per
          // row.
          for (PO po2 : triplemap2.getPos()) {
            JDBCTermMap tm1 = getTermMap(po1, f1);
            JDBCTermMap tm2 = getTermMap(po2, f2);
            if (tm1.getCompChecker().isCompatible(tm2)) {
              // they are compatible! we keep!
              toRetain.add(po1);

            }
          }
        }
      }
      mergedSomething = triplemap1.getPos().retainAll(toRetain);

      if (triplemap1.getPos().size() == 0) {
        triplemaps1.remove(triplemap1);
      }
    }
    return mergedSomething;

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
}