package org.aksw.sparqlmap.core.mapper.finder;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.aksw.sparqlmap.core.ImplementationException;
import org.aksw.sparqlmap.core.TranslationContext;
import org.aksw.sparqlmap.core.mapper.translate.QuadVisitorBase;
import org.aksw.sparqlmap.core.r2rml.JDBCTermMap;
import org.aksw.sparqlmap.core.r2rml.JDBCQuadMap;
import org.aksw.sparqlmap.core.r2rml.JDBCMapping;
import org.aksw.sparqlmap.core.r2rml.BoundQuadMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import util.QuadPosition;
import util.JenaHelper;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpWalker;
import com.hp.hpl.jena.sparql.algebra.op.OpJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpLeftJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpQuadPattern;
import com.hp.hpl.jena.sparql.algebra.op.OpTable;
import com.hp.hpl.jena.sparql.algebra.op.OpUnion;
import com.hp.hpl.jena.sparql.algebra.table.TableUnit;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.sparql.expr.Expr;

import static util.JenaHelper.*;


/**
 * this class generates a MappingBinding for a query by walking all over it.
 * @author joerg
 *
 */
public class Binder {
	private static Logger log = LoggerFactory.getLogger(Binder.class);
	
	private TranslationContext tc;
	private JDBCMapping mapconf;

	
	public Binder(JDBCMapping mappingConf, TranslationContext tc) {
		this.mapconf = mappingConf;
		this.tc = tc;
	}


	public MappingBinding bind(Op op){
	  
	  MappingBinding binding = new MappingBinding(tc.getQueryInformation().getFiltersforvariables(),(Collection) mapconf.getTripleMaps());
		
		
		OpWalker.walk(op, new BinderVisitor(binding));
		
		

		return new MappingBinding(bindingMap);
	}
	
	
	
	private class BinderVisitor extends QuadVisitorBase{
		
	  private MappingBinding binding;
		
		public BinderVisitor(
				MappingBinding binding) {
			this.binding = binding;
		}


		// we use this stack to track track which what to merge on unions, joins and left joins
		Stack<Collection<Quad>> quads = new Stack<Collection<Quad>>();
		
		
		@Override
		public void visit(OpJoin opJoin) {
			log.debug("Visiting opJoin " + opJoin);
			Collection<Quad> rightSideTriples = quads.pop();

			Collection<Quad> leftSideTriples = quads.pop();
			
			
			//we now merge the bindings for each and every triple we got here.
			
			boolean changed = binding.mergeBinding(partitionBindings(leftSideTriples), partitionBindings(rightSideTriples));
			changed = changed || binding.mergeBinding(partitionBindings(rightSideTriples), partitionBindings(leftSideTriples));

			//if we modified any binding, we have to walk this part of the Op-Tree again.
			
			if(changed){
				OpWalker.walk(opJoin, this);

			}
			mergeAndPutOnStack(leftSideTriples, rightSideTriples);
		}

		@Override
		public void visit(OpLeftJoin opLeftJoin) {
			log.debug("Visiting opLeftJoin"+opLeftJoin);
			
			if(opLeftJoin.getLeft() instanceof OpTable && ((OpTable)opLeftJoin.getLeft()).getTable() instanceof TableUnit){
				//leftjoin without triples. do nothing
				
			}else{
				Collection<Quad> rightSideTriples = quads.pop();
				Collection<Quad> leftSideTriples = quads.pop();
				//we now merge the bindings for each and every triple we got here.
				
				boolean changed =  binding.mergeBinding(partitionBindings(rightSideTriples), partitionBindings(leftSideTriples));
	
				//if we modified any binding, we have to walk this part of the Op-Tree again.
				
				if(changed){
					OpWalker.walk(opLeftJoin, this);
				}
				mergeAndPutOnStack(leftSideTriples, rightSideTriples);
			}
		}
		
		
		
		@Override
		public void visit(OpUnion opUnion) {
			log.debug("Visiting opUnion" + opUnion);
			//just popping the triples, so they are not used later on 
			Collection<Quad> rightSideTriples = quads.pop();
			Collection<Quad> leftSideTriples = quads.pop();
			
			mergeAndPutOnStack(leftSideTriples, rightSideTriples);
			
			
		}

		private void mergeAndPutOnStack(Collection<Quad> leftSideTriples,
				Collection<Quad> rightSideTriples) {
			//do not nothing to the triples but put them together, so they can be merged by a later join
			Collection<Quad> combined = new HashSet<Quad>();
			combined.addAll(leftSideTriples);
			combined.addAll(rightSideTriples);
			quads.add(combined);
		}
		
		
		
		
		@Override
		public void visit(OpQuadPattern opQuadBlock) {
			quads.add(opQuadBlock.getPattern().getList());
			
			for(Quad quad: opQuadBlock.getPattern().getList()){
					binding.init(quad);
			
			}
			
			// now merge them
			boolean hasMerged = false;
			do{
				hasMerged = mergeBinding(partitionBindings(opQuadBlock.getPattern().getList()), partitionBindings(opQuadBlock.getPattern().getList()));
			}while(hasMerged);
			
		}
		
		
		/**creates a subset of the bindings
		 * 
		 * @return
		 */
		private Map<Quad,Collection<JDBCQuadMap>> partitionBindings(Collection<Quad> quads){
			Map<Quad,Collection<JDBCQuadMap>> subset = new HashMap<Quad, Collection<JDBCQuadMap>>();
			for(Quad quad : quads){
				subset.put(quad, binding.get(quad));
			}
			
			return subset;
		}


		
		
	  
	  @Override
    public
	  void visit(OpTable opTable){
	    
	    if(opTable.getTable() instanceof TableUnit){
	      // do nothing here
	    }else{
	      throw new ImplementationException("Values/Table not implmeneted");

	    }

	  }
	  
		
		
	}
	
	

	
	
	
	
	
	


	


}
