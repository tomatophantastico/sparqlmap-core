package org.aksw.sparqlmap.core.r2rml;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import net.sf.jsqlparser.statement.select.FromItem;
/**
 * This representation of a triples map is bound to the underlying JDBC model.
 * Therefore it is bound to a live database.
 * @author joerg
 *
 */
public class JDBCTripleMap {
	
	static int nameCounter = 1;
	
	private String uri;
	public FromItem from;
	private JDBCTermMap subject;
	private Set<PO> pos = new LinkedHashSet<PO>();
	private JDBCTermMap graph;
	
	
	
	public JDBCTripleMap(String uri, FromItem from) {
		super();
		this.setUri(uri);
		this.from = from;
	}
	

	
	public void addPO(JDBCTermMap predicate,JDBCTermMap object){
		PO po = new PO(this);
		po.setPredicate(predicate);
		po.setObject(object);
		pos.add(po);
	}
	
	public Set<PO> getPos() {
		return pos;
	}


	public class PO{
		
		private JDBCTripleMap tripleMap;
		private JDBCTermMap predicate;
		private JDBCTermMap object;
		
		
		
		public PO(JDBCTripleMap tripleMap) {
			super();
			this.tripleMap = tripleMap;
		}
		public JDBCTripleMap getTripleMap() {
			return tripleMap;
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
		
		@Override
		public String toString() {
			
			return predicate.toString() + " " + object.toString();
		}
		
	}
	
	public FromItem getFrom() {
		return from;
	}
	
	public JDBCTermMap getGraph() {
		return graph;
	}
	
	public void setGraph(JDBCTermMap graph) {
		this.graph = graph;
	}
	
	public void setFrom(FromItem from) {
		this.from = from;
	}
	
		
	public JDBCTermMap getSubject() {
		return subject;
	}
	public void setSubject(JDBCTermMap subject) {
		this.subject = subject;
	}
	
	public JDBCTripleMap getShallowCopy(){
		JDBCTripleMap copy = new JDBCTripleMap(this.getUri(),this.from);
		copy.setGraph(graph);
		copy.pos = new HashSet<JDBCTripleMap.PO>(pos);
		copy.subject = subject;
		return copy;		
	}
	
	public JDBCTripleMap getDeepCopy(){
		JDBCTripleMap copy = new JDBCTripleMap(this.getUri(), this.from);
		copy.setGraph(graph);
		copy.subject = subject.clone("");
		for(PO po:pos){
			copy.addPO(po.getObject().clone(""), po.getPredicate().clone(""));
		}
		
		return copy;
	}
	
	@Override
	public String toString() {
		
		return "TripleMap: " + uri;
	}

	public String getUri() {
		return uri;
	}

	public void setUri(String uri) {
		this.uri = uri;
	}
	
	

}
