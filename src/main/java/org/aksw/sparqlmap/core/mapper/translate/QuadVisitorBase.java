package org.aksw.sparqlmap.core.mapper.translate;

import org.aksw.sparqlmap.core.ImplementationException;

import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpVisitorBase;
import com.hp.hpl.jena.sparql.algebra.OpWalker;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.algebra.op.OpGraph;
import com.hp.hpl.jena.sparql.algebra.op.OpQuadBlock;
import com.hp.hpl.jena.sparql.algebra.op.OpQuadPattern;
import com.hp.hpl.jena.sparql.expr.E_Exists;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprFunction1;
import com.hp.hpl.jena.sparql.expr.ExprFunctionOp;
import com.hp.hpl.jena.sparql.expr.ExprVisitorBase;

public class QuadVisitorBase extends OpVisitorBase{
  
  
  private boolean visitExists = true;
  
  
  public void setVisitExists(boolean visitExists) {
    this.visitExists = visitExists;
  }
  
	
	@Override
	public void visit(OpBGP opBGP) {
		throw new ImplementationException("Move to quad");
	}
	
	
	@Override
	public void visit(OpGraph opGraph) {
		throw new ImplementationException("Move to quad");
	}
	
		
	@Override
	public void visit(OpQuadBlock quadBlock) {
		for(OpQuadPattern pattern : quadBlock.convert()){
			visit(pattern);
		}
	}
	
	
	
	@Override
	public void visit(OpFilter opFilter) {
	  //check the filter for exists
	  
	  for(Expr expr: opFilter.getExprs().getList()){
	    expr.visit(new OpInExpressionBridge(this));
	  }
	 
	  super.visit(opFilter);
	  
	  
	  
	}
	
	
	
	private class OpInExpressionBridge extends ExprVisitorBase{
	  
	  
	  QuadVisitorBase quadvisitor;
	  
	  
	 
	  public OpInExpressionBridge(QuadVisitorBase quadvisitor) {
      super();
      this.quadvisitor = quadvisitor;
    }


    @Override
	  public void visit(ExprFunctionOp func) {
      if(visitExists){
        OpWalker.walk(func.getGraphPattern(), quadvisitor);
      }
      
	  }
	  
	  
	  
	  
	}
	
	
	

	
}
