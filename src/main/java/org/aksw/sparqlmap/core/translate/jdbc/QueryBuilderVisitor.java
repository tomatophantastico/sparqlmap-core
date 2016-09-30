package org.aksw.sparqlmap.core.translate.jdbc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.aksw.sparqlmap.core.ImplementationException;
import org.aksw.sparqlmap.core.TranslationContext;
import org.aksw.sparqlmap.core.r2rml.jdbc.JDBCColumnHelper;
import org.aksw.sparqlmap.core.r2rml.jdbc.JDBCQuadMap;
import org.aksw.sparqlmap.core.r2rml.jdbc.JDBCTermMap;
import org.aksw.sparqlmap.core.r2rml.jdbc.JDBCTermMapBinder;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpDistinct;
import org.apache.jena.sparql.algebra.op.OpExtend;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.algebra.op.OpGraph;
import org.apache.jena.sparql.algebra.op.OpGroup;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpLeftJoin;
import org.apache.jena.sparql.algebra.op.OpOrder;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.algebra.op.OpQuadPattern;
import org.apache.jena.sparql.algebra.op.OpSequence;
import org.apache.jena.sparql.algebra.op.OpSlice;
import org.apache.jena.sparql.algebra.op.OpTable;
import org.apache.jena.sparql.algebra.op.OpUnion;
import org.apache.jena.sparql.algebra.table.TableUnit;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprAggregator;
import org.apache.jena.sparql.expr.ExprFunctionOp;
import org.apache.jena.sparql.expr.ExprVar;
import org.apache.jena.sparql.expr.aggregate.AggCountVar;
import org.apache.jena.sparql.expr.aggregate.AggCountVarDistinct;
import org.apache.jena.sparql.expr.aggregate.Aggregator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.OrderByExpressionElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.SubSelect;
public class QueryBuilderVisitor extends QuadVisitorVocal {
	
	
	private ExpressionConverter exprconv;
	//defines if the filters should be pushed into the unions
	private boolean pushFilters = true;
	

	private static Logger log = LoggerFactory
			.getLogger(QueryBuilderVisitor.class);
	
	private Map<PlainSelect, PlainSelectWrapper> selectBody2Wrapper = new HashMap<PlainSelect, PlainSelectWrapper>();
	
	
	//this stack is also required by the expression converter, for EXISTS and NOT EXISTS
	protected Stack<PlainSelect> selects = new Stack<PlainSelect>();
	
	protected DataTypeHelper dataTypeHelper;
	protected final FilterUtil filterUtil;
	protected final TranslationContext translationContext;
  protected JDBCTermMapBinder tmf;

	public QueryBuilderVisitor(TranslationContext translationContext,	DataTypeHelper dataTypeHelper, ExpressionConverter expressionConverter, FilterUtil filterUtil, JDBCTermMapBinder tmf) {
		this.filterUtil = filterUtil;
		this.dataTypeHelper = dataTypeHelper;
		this.exprconv = expressionConverter;
		this.translationContext = translationContext;
		this.tmf = tmf;
	}

	@Override
	public void visit(OpUnion opUnion) {

		
		
		PlainSelectWrapper ps1 = (PlainSelectWrapper) selectBody2Wrapper
				.get(selects.pop());
		PlainSelectWrapper ps2 = (PlainSelectWrapper) selectBody2Wrapper
				.get(selects.pop());
		
		if(ps1.isAllNull()&& ps2.isAllNull()){
			log.debug("For union, both queries are empty, simply taking the first");
			selects.push(ps1.getSelectBody());
			
		}else if (ps1.isAllNull()){
			log.info("found empty select for union, skipping it");
			selects.push(ps2.getSelectBody());
			
		}else if(ps2.isAllNull()){
			log.info("found empty select for union, skipping it");
			selects.push(ps1.getSelectBody());
		}else{
			UnionWrapper union = new UnionWrapper(dataTypeHelper);
			union.addPlainSelectWrapper(ps1);
			union.addPlainSelectWrapper(ps2);
			
			PlainSelectWrapper ps = new PlainSelectWrapper(selectBody2Wrapper, dataTypeHelper, filterUtil, translationContext);
			ps.addSubselect(union, false);
			selects.push(ps.getPlainSelect());
		}
	}
	@Override
	public void visit(OpTable opTable){
	  if(opTable.getTable() instanceof TableUnit){
	    selects.push(new DummyBody());
	  }else{
	    throw new ImplementationException("Function OpTable not implmeneted");
	  }

	}

	
	


	@Override
	public void visit(OpLeftJoin opLeftJoin) {

		
		PlainSelect mainsb = selects.pop();
		PlainSelectWrapper main = null;
		if(! (mainsb instanceof DummyBody)){
			main = (PlainSelectWrapper) selectBody2Wrapper
					.get(mainsb);
		}
		if(opLeftJoin.getExprs()!=null){
		  processFilterExpressions(opLeftJoin.getExprs().getList());
		}
    SelectBody leftsb =selects.pop();
		PlainSelectWrapper left = (PlainSelectWrapper) selectBody2Wrapper
					.get(leftsb);
	
		//we check if select body has a from item
		if(((PlainSelect)leftsb).getFromItem() == null){
		   log.debug("ignoring unbound left join");
		}else if(main ==null){
			main = left;
			main.setOptional(true);
			
		}else{
			main.addSubselect(left,true);
		}

		
//        if(!selects.isEmpty()){
//        	SelectBody stackUnder = selects.pop();
//    		selects.push(main.getSelectBody());
//    		selects.push(stackUnder);
//        }else{
//
//        }
		if(main==null){
		  selects.push(mainsb);
		}else{
	    selects.push(main.getSelectBody());

		}

	}
	
	
	
	
	@Override
	public void visit(OpFilter opfilter){
	  super.visit(opfilter);
	  processFilterExpressions(opfilter.getExprs().getList());
				
	}
	
	private void processFilterExpressions(List<Expr> exprs){
    Wrapper wrap  =  this.selectBody2Wrapper.get(selects.peek());
    
    if(wrap instanceof UnionWrapper){
      UnionWrapper unionWrap = (UnionWrapper) wrap;
      boolean unpushable = true;
      for(Expr toPush : exprs){
        
        Set<String> filterVars = new HashSet<String>();
        for(Var var: toPush.getVarsMentioned()){
          filterVars.add(var.getName());
        }
        unpushable = pushIntoUnion(toPush, filterVars, unionWrap);

      }

      if(unpushable){
        //so the filter could not be pushed, we need to evaluate in the upper select
        //we have to wrap the union uinto a plainselect;
        PlainSelectWrapper ps = new PlainSelectWrapper(selectBody2Wrapper, dataTypeHelper, filterUtil, translationContext);
        ps.addSubselect(unionWrap, false);
        
        ps.addFilterExpression(convert(exprs,ps));
        selects.pop();
        selects.push(ps.getSelectBody());
        
      }
      
      
      
    }else{
      PlainSelectWrapper pswrap = (PlainSelectWrapper) wrap;
      if(this.pushFilters == true && pswrap.getSubselects().size()>0){
        // try to stuff everything into the unions
        for(Expr toPush : exprs){
          
          Set<String> filterVars = new HashSet<String>();
          for(Var var: toPush.getVarsMentioned()){
            filterVars.add(var.getName());
          }
        
          
          
          boolean unpushable = false;
          for(SubSelect subselect :pswrap.getSubselects().keySet()){
            Wrapper subSelectWrapper = pswrap.getSubselects().get(subselect);
            if(subSelectWrapper instanceof UnionWrapper){
              unpushable = pushIntoUnion(toPush, filterVars,subSelectWrapper);
              
              
            }else{
              // do nothing else, here be dragons
              unpushable = true;
            }
          }
          if(unpushable){
            //so the filter could not be pushed, we need to evaluate in the upper select
            pswrap.addFilterExpression(convert(Lists.newArrayList(toPush),pswrap));
          }
        }
        
        
        
        
      }else{
        //no filter pushing, just put it in
        
        pswrap.addFilterExpression(convert(exprs, pswrap));
      }
    }

  }

	private boolean pushIntoUnion(Expr toPush, Set<String> filterVars,
			Wrapper subSelectWrapper) {
		boolean unpushable = false;
		
		if(!(toPush instanceof ExprFunctionOp)){
		
  		//do that now for all plainselects of the union
  		for(PlainSelect ps: ((UnionWrapper)subSelectWrapper).getUnion().getPlainSelects()){
  			PlainSelectWrapper psw = (PlainSelectWrapper) selectBody2Wrapper.get(ps);
  			Set<String> pswVars = psw.getVarsMentioned();
  			if(pswVars.containsAll(filterVars)){
  				// if all filter variables are covered by the triples of the subselect, it can answer it.
  				psw.addFilterExpression(convert(Arrays.asList(toPush),psw));
  			}else if(Collections.disjoint(filterVars, pswVars)){
  				//if none are shared, than this filter simply does not matter for this wrapper 
  			}else{
  				//if there only some variables of the filter covered by the wrapper, answering in the top opration is neccessary.
  				unpushable = true;
  			}
  			
  
  		}
		}
		return unpushable;
	}
	
	
	private Collection<Expression> convert(Collection<Expr> sparqlExprs, PlainSelectWrapper psw){
	  Collection<Expression> sqlExpr = Lists.newArrayList();
	  for(Expr sparqlExpr: sparqlExprs){
	    sqlExpr.add(exprconv.asFilter(sparqlExpr,psw.getVar2TermMap(), this));
	  }
	  return sqlExpr;
	  
	}
	
	

	@Override
	public void visit(OpQuadPattern opQuad) {

		PlainSelectWrapper bgpSelect = new PlainSelectWrapper(selectBody2Wrapper,dataTypeHelper,filterUtil, translationContext);

		// PlainSelect bgpSelect = new PlainSelect();

		// for each triple we either calculate a from item, that is either one
		// column
		// of a table or a subselect if more than one column of a table were
		// applicable or we add the col directly to the bgpSelect

		for (Quad quad : opQuad.getPattern().getList()) {
			addTripleBindings(bgpSelect,quad,false);
		}

		this.selects.push(bgpSelect.getSelectBody());

		// TODO Auto-generated method stub
		super.visit(opQuad);
	}
	
	
	
	
	private void addTripleBindings( PlainSelectWrapper psw, Quad quad, boolean isOptional) {
		Collection<JDBCQuadMap> trms = translationContext.getQueryBinding().getBindingMap().get(quad);
		

		// do we need to create a union?
		if(trms.size()==1&&trms.iterator().next().getPos().size()==1&&filterUtil.getOptConf().optimizeSelfJoin){
			JDBCTripleMap trm = trms.iterator().next();
			PO po = trm.getPos().iterator().next();
			//no we do not need
			psw.addTripleQuery(trm.getGraph(),quad.getGraph().getName(), trm.getSubject(), quad
					.getSubject().getName(), po.getPredicate(),quad
					.getPredicate().getName(),po.getObject(),quad.getObject().getName(), isOptional);
			if(translationContext.getQueryInformation().isProjectionPush()){
				//psw.setDistinct(true);
				psw.setLimit(1);
			}
	
			
		}else if(trms.size()==0){
			// no triple maps found.
			//bind to null values instead.
			psw.addTripleQuery(JDBCTermMap.createNullTermMap(dataTypeHelper),quad.getGraph().getName(),JDBCTermMap.createNullTermMap(dataTypeHelper), quad
					.getSubject().getName(), JDBCTermMap.createNullTermMap(dataTypeHelper),quad
					.getPredicate().getName(),JDBCTermMap.createNullTermMap(dataTypeHelper),quad.getObject().getName(), isOptional);
			
			
		}else{
			List<PlainSelectWrapper> pselects = new ArrayList<PlainSelectWrapper>();

			//multiple triple maps, so we contruct a union
			
			
			for (JDBCTripleMap trm : trms) {
				for (PO po : trm.getPos()) {

					PlainSelectWrapper innerPlainSelect = new PlainSelectWrapper(this.selectBody2Wrapper,dataTypeHelper,filterUtil, translationContext);
					//build a new sql select query for this pattern
					innerPlainSelect.addTripleQuery(trm.getGraph(),quad.getGraph().getName(),trm.getSubject(), quad
							.getSubject().getName(), po.getPredicate(),quad
							.getPredicate().getName(),po.getObject(),quad.getObject().getName(), isOptional);
					if(translationContext.getQueryInformation().isProjectionPush()){
						//innerPlainSelect.setDistinct(true);
						innerPlainSelect.setLimit(1);
					}
					pselects.add(innerPlainSelect);
					
				}
			}
			
			UnionWrapper union = new UnionWrapper(dataTypeHelper);
			for (PlainSelectWrapper plainSelectWrapper : pselects) {
				union.addPlainSelectWrapper(plainSelectWrapper);

			}
			
			psw.addSubselect(union,isOptional);
		}

	}

	
	public void visit(OpJoin opJoin) {
		
		PlainSelect left = this.selects.pop();
		PlainSelect right = this.selects.pop();
		
		PlainSelectWrapper leftWrapper = (PlainSelectWrapper) this.selectBody2Wrapper.get(left);
		PlainSelectWrapper rightWrapper = (PlainSelectWrapper) this.selectBody2Wrapper.get(right);
		leftWrapper.addSubselect(rightWrapper, false);
		this.selects.push(left);
		
		
		
	}
	
	
	
	

	
	@Override
	public void visit(OpBGP opBGP) {
		log.error("implement opBGP");
	}
	
	
	@Override
	public void visit(OpGraph opGraph) {
		log.error("implement opGRaph");
		
	}
	

	@Override
	public void visit(OpExtend opExtend) {
	  
	  	  
	  PlainSelect ps = this.selects.peek();
	  PlainSelectWrapper psw = this.selectBody2Wrapper.get(ps);
	  
	  if(opExtend.getVarExprList().size()!=1){
	    throw new ImplementationException("Bind/extend encountered with multiple expressions, however only one is supported.");
	  }
	  
	  Var var =opExtend.getVarExprList().getVars().get(0);
	  Expr expr = opExtend.getVarExprList().getExpr(var);
	     
	  
	  JDBCTermMap termMap = this.exprconv.asTermMap(expr, psw.getVar2TermMap(),null);
	  
	  selectBody2Wrapper.get(ps).putTermMap(termMap, var.getName(), false);
	  
		
	}



	@Override
	public void visit(OpSequence opSequence) {
	  
	  
	  //performing a join here
    PlainSelect left = this.selects.pop();
    PlainSelect right = this.selects.pop();
    
    PlainSelectWrapper leftWrapper = (PlainSelectWrapper) this.selectBody2Wrapper.get(left);
    PlainSelectWrapper rightWrapper = (PlainSelectWrapper) this.selectBody2Wrapper.get(right);
    leftWrapper.addSubselect(rightWrapper, false);
    this.selects.push(left);
    
    
		
	}


	@Override
	public void visit(OpOrder opOrder) {
		super.visit(opOrder);
		// gets ignored here
		
		
	}

	@Override
	public void visit(OpProject opProject) {
		super.visit(opProject);
		//projection is managed in the getSQL method
		
	}



	@Override
	public void visit(OpDistinct opDistinct) {
		//can be ignored here
		
	}

	@Override
	public void visit(OpSlice opSlice) {
		super.visit(opSlice);
		//ignore here
		
	}

	

	@Override
	public void visit(OpGroup opGroup) {
	  
	  PlainSelect ps = this.selects.peek();
    PlainSelectWrapper psw = this.selectBody2Wrapper.get(ps);
    
	  
	  if(opGroup.getAggregators().size()!=1){
	    throw new ImplementationException("More than one aggregate expression encountered.");
	  }
	  
	  ExprAggregator exprAggregate =  opGroup.getAggregators().get(0);
	  
	  Aggregator aggregate = exprAggregate.getAggregator();
	  ExprVar aggVar =  exprAggregate.getAggVar();
	  
	  Function aggregateFunction = new Function();
	  List<Expression> exprList = Lists.newArrayList();
	  
	  
	  if(aggregate instanceof AggCountVar){
	    AggCountVar aggCountVar = (AggCountVar) aggregate;
	    ExprVar countVar = (ExprVar) aggCountVar.getExpr();
	    JDBCTermMap countTermMap = psw.getVar2TermMap().get(countVar.getVarName());
	    
	    if(countTermMap.getColumns().isEmpty()){
	      throw new ImplementationException("count on non-col based var needs subquery");
	    }else{
	      exprList.add(countTermMap.getColumns().get(0));

	    }
	    
	    aggregateFunction.setName("count");
	   
	   
	    	    
	  }else if (aggregate instanceof AggCountVarDistinct){
	    aggregateFunction.setName("count");
	    aggregateFunction.setDistinct(true);
	    
	  }else{
	    throw new ImplementationException("Unimplemented Aggregate Function:" + aggregate.toString());
	  }
	  aggregateFunction.setParameters(new ExpressionList(exprList));
	  
	
	  
	  JDBCTermMap countTm = tmf.createNumericalTermMap(aggregateFunction, XSDDatatype.XSDinteger);
	  
	  psw.putTermMap(countTm, aggVar.getVarName(), false);
	  
	  //opGroup.getAggregators()
	 
	}
	 
  public static class DummyBody extends PlainSelect{

    @Override
    public void accept(SelectVisitor selectVisitor) {
      
    }
    
    
    @Override
    public List<SelectItem> getSelectItems() {
      return Lists.newArrayList();
    }
    
  }
  
  public Select getSqlQuery() {
    if (this.selects.size() != 1) {
      log.error("Currently on the stack ist:");
      for(PlainSelect ps : selects){
        log.error(ps.toString());
      }
      
      
      throw new RuntimeException("Stack not stacked properly, stack size is:" + selects.size() );
    }

    // we need to enforce projection and slicing & ordering

    SelectBody sb = selects.pop();
    PlainSelect toModify = null;

    Map<String, JDBCTermMap> var2termMap = Maps.newHashMap();
    
    if(sb instanceof SetOperationList){
      
    
      PlainSelectWrapper wrap = new PlainSelectWrapper(selectBody2Wrapper,dataTypeHelper,filterUtil, translationContext);
      
      wrap.addSubselect(this.selectBody2Wrapper
          .get(sb), false);
      sb = wrap.getSelectBody();
    }
    
    
//    if (sb instanceof PlainSelect) {
      toModify = (PlainSelect) sb;
      
      List<SelectExpressionItem> removeseis = new ArrayList<SelectExpressionItem>();
      
      List<String> projectVars = new ArrayList<String>();
      
      for(Var var: translationContext.getQueryInformation().getProject()
          .getVars()){
        projectVars.add(var.getName());
      }
          

      for(Object o_sei:  ((PlainSelect) sb).getSelectItems()){
        SelectExpressionItem sei = (SelectExpressionItem) o_sei;
        String varname = JDBCColumnHelper.colnameBelongsToVar(sei.getAlias());
        //eventuall clean from deunion
        if(varname.contains("-du")){
          varname = varname.substring(0, varname.length()-5);
          }
        
        if(!projectVars.contains(varname)){
          removeseis.add(sei);
        }
      }
      
      ((PlainSelect) sb).getSelectItems().removeAll(removeseis);
      
      PlainSelectWrapper psw = ((PlainSelectWrapper) selectBody2Wrapper.get(sb));
      if(psw!=null){
         var2termMap = psw.getVar2TermMap();
      }


    if (translationContext.getQueryInformation().getOrder() != null && toModify.getOrderByElements() == null) {
      // if the list is not set, we create a new set
      List<OrderByElement> obys = exprconv.convert(
          translationContext.getQueryInformation().getOrder(), var2termMap);
      int i = 0;
      for (OrderByElement orderByElement : obys) {
        
        
        if(orderByElement instanceof OrderByExpressionElement){
          //we check, if the appropriate expression is there.
          OrderByExpressionElement obx = (OrderByExpressionElement) orderByElement;
          Expression obExpr = obx.getExpression();
          Expression matchingExp = null;
          for (Object osei : toModify.getSelectItems()) {
            SelectExpressionItem sei  = (SelectExpressionItem) osei; 
            if(sei.getExpression().toString().equals(obExpr.toString())){
              matchingExp = sei.getExpression();
              break;
            }
            
          }
          
          if(matchingExp==null){
            //we need to add the order by column to the select item
            SelectExpressionItem obySei = new SelectExpressionItem();
            obySei.setAlias(JDBCColumnHelper.COL_NAME_LITERAL_NUMERIC + i++);
            obySei.setExpression(obExpr);
            toModify.getSelectItems().add(obySei);
          }
        }
        
      }
      
      
      toModify.setOrderByElements(obys);
    }

    if (translationContext.getQueryInformation().getSlice() != null && toModify.getLimit() == null) {
      
      
      toModify = dataTypeHelper.slice(toModify,translationContext.getQueryInformation().getSlice());
      

    }
    
    
    if(translationContext.getQueryInformation().getDistinct()!=null){
      toModify.setDistinct(new Distinct());
    }

    Select select = new Select();
    select.setSelectBody(toModify);

    return select;
  }


}
