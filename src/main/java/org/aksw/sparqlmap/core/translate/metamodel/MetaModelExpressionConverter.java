package org.aksw.sparqlmap.core.translate.metamodel;

import java.util.List;
import java.util.Map;
import java.util.Stack;

import net.sf.jsqlparser.expression.ExpressionVisitor;

import org.aksw.sparqlmap.core.r2rml.TermMap;
import org.aksw.sparqlmap.core.r2rml.TermMapColumn;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.FromItem;
import org.apache.metamodel.query.OperatorType;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.util.BaseObject;

import com.google.common.collect.Lists;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.expr.E_Equals;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprFunction2;
import org.apache.jena.sparql.expr.ExprVar;
import org.apache.jena.sparql.expr.ExprVisitorBase;
import org.apache.jena.sparql.expr.ExprWalker;
import org.apache.jena.sparql.expr.NodeValue;

public class MetaModelExpressionConverter {
  
  
  private Map<String,TermMap> var2termMap;
  
  
  
  public MetaModelExpressionConverter(Map<String, TermMap> var2termMap) {
    super();
    this.var2termMap = var2termMap;
  }


  public FilterItem convertFilter(OpFilter opFilter){
    List<FilterItem> fis = Lists.newArrayList();
    
    for(Expr expr: opFilter.getExprs().getList()){
      FilterConverter conv = new FilterConverter();
      
      ExprWalker.walk(conv, expr);
      if(conv.fi != null &&conv.siStack.isEmpty() )
      fis.add(conv.fi);
      
      
    }
    FilterItem result = new FilterItem(fis);
    
    return result;
    
  }
  
  
  private class FilterConverter extends ExprVisitorBase{
    Stack<Object> siStack = new Stack<Object>();
    boolean incompatibleExpression = false;
    

    
    
    
    @Override
    public void visit(ExprFunction2 func) {
      // shortcut
      if(!incompatibleExpression){
        return;
      }
      // for metamodel, selectitem goes first
      Object obj1 = siStack.pop();
      Object obj2 = siStack.pop();
      boolean reverseOrder = false;
      ExprVar si  = null;
      Object obj = null;
      if(obj1 instanceof SelectItem){
        si = (ExprVar) obj1;
        obj = obj2;
      }else if(obj2 instanceof SelectItem){
        si = (ExprVar) obj2;
        obj = obj1;
        reverseOrder = true;
      }
      
      
      FilterItem fi = null;
      
      if(func instanceof E_Equals){
        if(si!=null){
          
          
          
          
         fi = new FilterItem(si,OperatorType.EQUALS_TO,obj);
        }
        
      }else{
        incompatibleExpression = true;
      }
      
      
      siStack.push(fi);
    }
   
    
   @Override
  public void visit(NodeValue nv) {
     
    siStack.push(nv);
  }
   
  @Override
  public void visit(ExprVar nv) {
   siStack.push(nv);
  } 
    
    
  }
  
  
  List<FromItem> compare(OperatorType type,TermMap termMap, SelectItem si, NodeValue nv){
    if(termMap instanceof TermMapColumn){
      FromItem = new 
      
    }
    
    
  }
  
  
  
  
  

}
