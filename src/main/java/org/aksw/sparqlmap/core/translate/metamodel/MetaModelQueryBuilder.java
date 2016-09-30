package org.aksw.sparqlmap.core.translate.metamodel;

import java.util.Map;

import org.aksw.sparqlmap.core.mapper.RightFirstWalker;
import org.aksw.sparqlmap.core.mapper.finder.MappingBinding;
import org.aksw.sparqlmap.core.r2rml.QuadMap;
import org.aksw.sparqlmap.core.translate.jdbc.QuadVisitorVocal;
import org.apache.metamodel.query.Query;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpWalker;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.algebra.op.OpQuadPattern;
import org.apache.jena.sparql.core.Quad;

public class MetaModelQueryBuilder extends QuadVisitorVocal{
  
 
  private Map<Quad, QuadMap> binding;
  private MetaModelContext mcontext;


  
  public MetaModelQueryBuilder(Map<Quad, QuadMap>  binding, MetaModelContext mcontext) {
    super();
    this.binding = binding;
    this.mcontext = mcontext;
    
  }

  
  Query build(Op query){
    QueryBuilderVisitor qbv = new QueryBuilderVisitor();
    RightFirstWalker.walk(query, qbv);
    
    return qbv.queryWrapper.getQuery();
    
  }

  
  
  private class QueryBuilderVisitor extends QuadVisitorVocal{
    
    MetaModelQueryWrapper queryWrapper = new MetaModelQueryWrapper(mcontext);
    
    

    
    @Override
    public void visit(OpQuadPattern quadPattern) {
      for(Quad quad : quadPattern.getPattern().getList()){
        queryWrapper.addQuad(quad, binding.get(quad), true);
      }
      
      
    }
    
    
    
    @Override
    public void visit(OpFilter opFilter) {
      // TODO Auto-generated method stub
      super.visit(opFilter);
    }
    
  }

  
}
