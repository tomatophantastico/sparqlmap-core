package org.aksw.sparqlmap.core.mapper.finder;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.aksw.sparqlmap.core.r2rml.JDBCTripleMap;
import org.aksw.sparqlmap.core.r2rml.JDBCTripleMap.PO;

import com.hp.hpl.jena.sparql.core.Quad;

/**
 * this class cis a wrapper around the triple bindings
 * 
 * @author joerg
 * 
 */
public class MappingBinding {

	private Map<Quad, Collection<JDBCTripleMap>> bindingMap = new HashMap<Quad, Collection<JDBCTripleMap>>();


	public MappingBinding(Map<Quad, Collection<JDBCTripleMap>> bindingMap) {
		this.bindingMap = bindingMap;
	}


	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Triple Bindings are: \n");
		Set<Quad> quads = this.bindingMap.keySet();
		for (Quad quad: quads) {
			sb.append("* " + quad.toString() + "\n");
			for (JDBCTripleMap tm : this.bindingMap.get(quad)) {
				sb.append("    Triplemap: " + tm + "\n");
				for (PO po : tm.getPos()) {
					sb.append("     PO:" + po.getPredicate().toString() + " "
							+ po.getObject().toString() + "\n");
				}
			}
		}
		return sb.toString();
	}
	
	
	public Map<Quad, Collection<JDBCTripleMap>> getBindingMap() {
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
}