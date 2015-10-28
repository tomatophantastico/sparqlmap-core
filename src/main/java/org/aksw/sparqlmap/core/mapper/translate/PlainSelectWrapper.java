package org.aksw.sparqlmap.core.mapper.translate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.util.BaseSelectVisitor;

import org.aksw.sparqlmap.core.TranslationContext;
import org.aksw.sparqlmap.core.r2rml.JDBCColumnHelper;
import org.aksw.sparqlmap.core.r2rml.JDBCTermMap;
import org.hamcrest.core.IsNull;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.hp.hpl.jena.sparql.expr.Expr;

public class PlainSelectWrapper implements Wrapper{
	
	public static final String SUBSEL_SUFFIX = "subsel_";

	private TranslationContext translationContext;
	
	private Map<PlainSelect, PlainSelectWrapper> registerTo;


	public PlainSelectWrapper(Map<PlainSelect, PlainSelectWrapper> registerTo,
			DataTypeHelper dth, ExpressionConverter exprconv,
			FilterUtil filterUtil, TranslationContext translationContext) {
		super();
		this.translationContext = translationContext;
		this.exprconv = exprconv;
		this.dth = dth;
		this.filterUtil = filterUtil;
		this.registerTo = registerTo;
	
			// init the Plain Select
		plainSelect = new PlainSelect();
		
		plainSelect.setSelectItems(new ArrayList<SelectItem>());
		plainSelect.setJoins(new ArrayList<Join>());
		registerTo.put(plainSelect, this);
			
		
	
	}


	static org.slf4j.Logger log = org.slf4j.LoggerFactory
			.getLogger(PlainSelectWrapper.class);

	private Multimap<String, EqualsTo> _fromItem2joincondition = LinkedListMultimap
			.create();


	private Map<String,JDBCTermMap> var2termMap = new LinkedHashMap<String,JDBCTermMap>();
	private Map<JDBCTermMap,String> termMap2var = new LinkedHashMap<JDBCTermMap,String>();
	

	private DataTypeHelper dth;

	private ExpressionConverter exprconv;

	private FilterUtil filterUtil;

	private PlainSelect plainSelect;

	private Map<SubSelect, Wrapper> subselects = new HashMap<SubSelect, Wrapper>();
	

	
	public void setDistinct(boolean distinct){
		if(distinct){
			this.plainSelect.setDistinct(new Distinct());
		}else{
			this.plainSelect.setDistinct(null);
		}
		
	}
	

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("*************\n");
		sb.append("JOIN Conditions are:\n");

		for (String fis : _fromItem2joincondition.keySet()) {
			sb.append("  FromItem: " + fis + "\n");
			for (EqualsTo eq : _fromItem2joincondition.get(fis)) {

				sb.append("         " + eq.toString() + "\n");
			}

		}

		return sb.toString();
	}

	public void setLimit(int i) {
		Limit lim = new Limit();
		lim.setRowCount(i);
		this.plainSelect.setLimit(lim);
	}
	
	
	
	Multimap<JDBCTermMap,JDBCTermMap> orig2dupClones = HashMultimap.create();
	Multimap<JDBCTermMap,JDBCTermMap> joins =  HashMultimap.create();
	//Multimap<TermMap,TermMap> optJoins =  HashMultimap.create();
	Set<JDBCTermMap> optionalTermMaps = new HashSet<JDBCTermMap>();



	




	private void createSelectExpressionItems() {
		this.plainSelect.setSelectItems(new ArrayList<SelectItem>());
		for(String var : var2termMap.keySet()){
			plainSelect.getSelectItems().addAll(var2termMap.get(var).getSelectExpressionItems(var));
		}
	}
	
	private void createJoins() {

		this.plainSelect.setFromItem(null);
		this.plainSelect.setJoins(new ArrayList<Join>());

		// all from ITtems mentioned so far.
		Set<String> fromItemAliases = new LinkedHashSet<String>();

	
		Multimap<Expression, String> fromItemAliasesOfExpression =  HashMultimap
				.create();

		for(String var: var2termMap.keySet()){
			JDBCTermMap tm = var2termMap.get(var);

			
			// calculate the join conditions we have to put into the join beside
			// the conditions defined in the mapping.

			mapTermMapToJoin(fromItemAliases, tm, fromItemAliasesOfExpression);

			// add the joins
			Collection<JDBCTermMap> joinginTermMaps = new ArrayList<JDBCTermMap>();
			
			//joinginTermMaps.addAll(this.optJoins.get(tm));
			
			joinginTermMaps.addAll( this.joins.get(tm));

			
			
			for(JDBCTermMap joinginTermMap : joinginTermMaps){
				
				

				JDBCTermMap equalTermMap = filterUtil.compareTermMaps( joinginTermMap,tm,
						EqualsTo.class);

				// attempt to break up this expression
				Expression termMapEquality = DataTypeHelper.uncast(equalTermMap
						.getLiteralValBool());

				List<Expression> decomposedTermMapEquality = new ArrayList<Expression>();
				FilterUtil.splitFilters(termMapEquality,
						decomposedTermMapEquality);

				extractRequiredFromItems(fromItemAliasesOfExpression,
						decomposedTermMapEquality);
				
				mapTermMapToJoin(fromItemAliases, joinginTermMap, fromItemAliasesOfExpression);
				
				

			}
		}
		
		if (!fromItemAliasesOfExpression.isEmpty()) {
			filters.addAll(fromItemAliasesOfExpression.keySet());
		}
		
	}


	public void extractRequiredFromItems(
			Multimap<Expression, String> fromItemAliasesOfExpression,
			Collection<Expression> decomposedTermMapEquality) {
		for (Expression eqExpression : decomposedTermMapEquality) {
			final List<String> fis = new ArrayList<String>();

			eqExpression.accept(new BaseSelectVisitor() {

				@Override
				public void visit(Table table) {
					super.visit(table);
					fis.add(table.getAlias());
				}
			});
			if(fis.size()>1){
				fromItemAliasesOfExpression.putAll(eqExpression, fis);	
			}
			
		}
	}


	public void mapTermMapToJoin(Set<String> fromItemAliases, JDBCTermMap tm,			
			Multimap<Expression, String> fromItemAliasesOfExpression) {
		
		
		
		Iterator<FromItem> fromIter = tm.getFromItems().iterator();
		
		extractRequiredFromItems(fromItemAliasesOfExpression, new HashSet<Expression>( tm.getFromJoins()));
		
		while (fromIter.hasNext()) {
			// his build the join between
			FromItem fi = fromIter.next();
			if (fromItemAliases.contains(fi.getAlias())) {
				// already in there, no need to add it again
				continue;
			}

			fromItemAliases.add(fi.getAlias());
			if (plainSelect.getFromItem() == null) {
				plainSelect.setFromItem(fi);
			} else {
				Join join = new Join();
				join.setRightItem(fi);

				// assemble the join condition here
				List<Expression> onExpressions = new ArrayList<Expression>();
				// get all expression that can be applied so far.
				Iterator<Expression> exprIter = fromItemAliasesOfExpression
						.keySet().iterator();
				for (; exprIter.hasNext();) {
					Expression expr = exprIter.next();
					Collection<String> exprFromItemAliases = fromItemAliasesOfExpression.get(expr);
					
					boolean allContained = true;
					for(String expFromItemAlias:exprFromItemAliases){
						if(!fromItemAliases.contains(expFromItemAlias)){
													
							allContained = false;
							break;
						}
					}
					
					if(allContained){
						
					
						onExpressions.add(expr);
						exprIter.remove();
					}
				}

				if (onExpressions.isEmpty()) {
					join.setSimple(true);
				} else {
					join.setOnExpression(new Parenthesis(FilterUtil
							.conjunct(onExpressions)));
				}
				
				if(optionalTermMaps.contains(tm)){
					join.setLeft(true);
				}

				plainSelect.getJoins().add(join);

			}
		}
		
	}
	
	

	// in this method we apply the filter there were not created by joins
	private void setFilter(){
		plainSelect.setWhere(null);
		
		// add joins as filters
		Set<Expression> filtersToAdd = new HashSet<Expression>(this.filters); 
//				for(TermMap left : joins.keySet()){
//					for(TermMap right: joins.get(left)){
//						TermMap joinCond = filterUtil.compareTermMaps(left, right, EqualsTo.class);
//						if(joinCond!=null){
//							filtersToAdd.add(joinCond.getLiteralValBool());	
//						}
//						
//					}
//				}

		if(plainSelect.getWhere()!=null){
			List<Expression> tmpExpressions = new ArrayList<Expression>();
			tmpExpressions.add(plainSelect.getWhere());
			tmpExpressions.addAll(filtersToAdd);
			plainSelect.setWhere(FilterUtil.conjunct(tmpExpressions));
			
		}else{
			plainSelect.setWhere(FilterUtil.conjunct(filtersToAdd));
		}
	}

	Set<Expression> filters = new HashSet<Expression>();
	
	public void addFilterExpression(Collection<Expr> exprs) {
		for(Expr expr:exprs){
			Expression filter = exprconv.asFilter(expr, var2termMap);
			if(filter!=null && !(filter instanceof NullValue)){
				this.filters.add(filter);
			}
			
		}
		
		
		
		createFilter();
		
	}
	
	private void createFilter() {
	  setFilter();
	  setNullForNonOptionals();
    
  }




	public Map<String, JDBCTermMap> getVar2TermMap() {
		return var2termMap;
	}
	

	public PlainSelect getPlainSelect() {
		

		return plainSelect;
	}

	public PlainSelect getSelectBody() {

		return getPlainSelect();
	}

	
	public List<SelectItem> getSelectExpressionItems() {
		return getPlainSelect().getSelectItems();
	}

	public Map<SubSelect, Wrapper> getSubselects() {
		return subselects;
	}

	
	public Set<String> getVarsMentioned() {

		return new LinkedHashSet<String>(this.getVar2TermMap().keySet());
	}
	
	
	

	public void addTripleQuery(JDBCTermMap origGraph, String graphAlias, JDBCTermMap origSubject, String subjectAlias,
			JDBCTermMap origPredicate, String predicateAlias, JDBCTermMap origObject,
			String objectAlias, boolean isOptional) {

		String suffix = "_" + subjectAlias;
		
		JDBCTermMap subject = origSubject.clone(suffix);	
		JDBCTermMap object = origObject.clone(suffix);
		JDBCTermMap predicate = origPredicate.clone(suffix);
		JDBCTermMap graph = origGraph.clone(suffix);
		
		if(needsDuplication(subject, subjectAlias) || needsDuplication(object, objectAlias)||needsDuplication(predicate, predicateAlias)||needsDuplication(graph, graphAlias)){
			
			String cloneSuffix = "_dup" + translationContext.getAndIncrementDuplicateCounter();
			subject  = subject.clone(cloneSuffix);
			object = object.clone(cloneSuffix);
			predicate = predicate.clone(cloneSuffix);
			graph  = graph.clone(cloneSuffix);
		}
		
		putTermMap(subject, subjectAlias, isOptional);
		putTermMap(object, objectAlias, isOptional);
		putTermMap(predicate, predicateAlias, isOptional);
		putTermMap(graph, graphAlias, isOptional);


	}
	
	
	
	
	public void putTermMap(JDBCTermMap termMap, String alias, boolean isOptional){
		
		mapTermMap(termMap, alias, isOptional);
		
		createJoins();
		createSelectExpressionItems();
		createFilter();

	}


	private void setNullForNonOptionals() {
		List<Expression> notnulls = new ArrayList<Expression>();
		for (JDBCTermMap tm : var2termMap.values()) {
			if (!optionalTermMaps.contains(tm)) {
			  for(Expression tocheck: tm.getExpressions()){
			    tocheck = DataTypeHelper.uncast(tocheck);
			    if(tocheck instanceof Column){
			      
			      IsNullExpression isnull = new IsNullExpression();
			      isnull.setNot(true);
			      isnull.setLeftExpression(tocheck);
			      notnulls.add(isnull);
			    }
			  }
			  
				
			}
		}
		if (!notnulls.isEmpty()) {
			Parenthesis parenthesis = new Parenthesis(
					FilterUtil.conjunct(notnulls));
			if (plainSelect.getWhere() != null) {
				AndExpression and = new AndExpression(plainSelect.getWhere(),
						parenthesis);
				plainSelect.setWhere(and);
			} else {
				plainSelect.setWhere(parenthesis);
			}
		}

	}
	
	
	private boolean needsDuplication(JDBCTermMap termMap, String alias){
		if(termMap2var.containsKey(termMap)){
			// term map already in use
			String varTermMapInUse = termMap2var.get(termMap);
			if(!varTermMapInUse.equals(alias)){
				// a different variable, a duplication is required.
				return true;
								
			
			}
			//else nothing is required
		}
		return false;
	}


	public void mapTermMap(JDBCTermMap termMap, String alias, boolean isOptional) {
		
		
		if(var2termMap.containsKey(alias)){
			//we add but we'll have to mark that as joins
			JDBCTermMap termMapInUse = var2termMap.get(alias);
//			if(isOptional){
//				optJoins.put(termMapInUse,termMap);
//			}else{
				joins.put(termMapInUse,termMap);
//			}
			


		
		}else{
			var2termMap.put(alias,termMap);
			termMap2var.put(termMap, alias);
		}
		
		//the join conditions have to be added anyways
		
		filters.addAll(termMap.getFromJoins());
		
		

		if(isOptional){
			optionalTermMaps.add(termMap);
		}
	}
	
  public void addSubselect(Wrapper right, boolean optional) {

    // check if the subselect queries only for a single triple and is
    // optional.
    // then we can add it directly to the plain select.

    if (filterUtil.getOptConf().optimizeSelfLeftJoin == true
        && right instanceof PlainSelectWrapper 
        && ((PlainSelectWrapper)right).getSubselects().isEmpty()
        && ((PlainSelectWrapper) right).getVarsMentioned().size() == 4
        && ((PlainSelectWrapper) right).subselects.size() == 0
        && this.getFromItemAliases().containsAll(((PlainSelectWrapper) right).getFromItemAliases())) {
      PlainSelectWrapper ps = (PlainSelectWrapper) right;

      // we require the not shared variable to be either constant resource
      // or a literal. column generated resources can be troublesome, if
      // generated from more than one column.
      for (String var : ps.getVarsMentioned()) {

        JDBCTermMap rightVarTc = ps.getVar2TermMap().get(var);
        // variables already there and equal can be ignored
        // if not equal, we cannot use this optimization
        JDBCTermMap thisVarTc = var2termMap.get(var);
        if (thisVarTc == null) {

          // add it if all from items are already in the plain select
          // we use the alias to check this.
          Set<String> fiAliases = new HashSet<String>();

          for (FromItem fi : rightVarTc.getFromItems()) {
            fiAliases.add(fi.getAlias());
          }

          if (getFromItemAliases().containsAll(fiAliases)) {
            putTermMap(rightVarTc, var, true);
          }
        }
      }
    } else {

      // create a new subselect
      SubSelect subsell = new SubSelect();
       //register it 
     
      
      subsell.setSelectBody(right.getSelectBody());
      subsell.setAlias(SUBSEL_SUFFIX
          + translationContext.getAndIncrementSubqueryCounter());

      Map<String, JDBCTermMap> rightVar2TermMap = null;

      if (right instanceof UnionWrapper) {
        UnionWrapper rightWrapper = (UnionWrapper) right;
        rightVar2TermMap = rightWrapper.getVar2TermMap(subsell.getAlias());
      } else {
        PlainSelectWrapper rightWrapper = (PlainSelectWrapper) right;
        rightVar2TermMap = rightWrapper.getVar2TermMap();
      }

      for (String var : rightVar2TermMap.keySet()) {

        // create a new subselect term for each term map in there.
        JDBCTermMap subselTermMap = createSubseletTermMap(
            rightVar2TermMap.get(var), var, subsell);
        putTermMap(subselTermMap, var, optional);
      }
      this.subselects.put(subsell, right);
    }
  }
	
	
	/**
	 * return all from items mentioned in this plainselectwrapper. Not including any fromitems mentioned in subqueries.
	 */
	public Set<String> getFromItemAliases(){
		
		Set<String> fis = new HashSet<String>();
		fis.add(this.plainSelect.getFromItem().getAlias());
		for(Join join : this.plainSelect.getJoins()){
			fis.add(join.getRightItem().getAlias());
		}
		return fis;
	}
	
	
	private JDBCTermMap createSubseletTermMap(JDBCTermMap orig,String var, FromItem subselect){
		
		
		
		List<Expression> expressions = new ArrayList<Expression>();
		
		for(SelectExpressionItem sei: orig.getSelectExpressionItems(var)){
			expressions.add( JDBCColumnHelper.createColumn(subselect.getAlias(), sei.getAlias()));
		}
		
		JDBCTermMap tm =  JDBCTermMap.createTermMap(dth, expressions);
		tm.addFromItem(subselect);
		return tm;
		
	}
	
	
	private boolean optional = false;
	

	


	public void setOptional(boolean optional) {
		this.optional  = optional;
		
	}
	
}
