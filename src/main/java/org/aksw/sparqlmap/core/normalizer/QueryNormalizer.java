package org.aksw.sparqlmap.core.normalizer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.aksw.sparqlmap.core.TranslationContext;
import org.aksw.sparqlmap.core.config.syntax.r2rml.ColumnHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.sparql.algebra.AlgebraGenerator;
import com.hp.hpl.jena.sparql.algebra.AlgebraQuad;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpVars;
import com.hp.hpl.jena.sparql.algebra.TransformCopy;
import com.hp.hpl.jena.sparql.algebra.Transformer;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.algebra.op.OpQuadBlock;
import com.hp.hpl.jena.sparql.algebra.op.OpQuadPattern;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.E_Datatype;
import com.hp.hpl.jena.sparql.expr.E_Equals;
import com.hp.hpl.jena.sparql.expr.E_LangMatches;
import com.hp.hpl.jena.sparql.expr.E_OneOf;
import com.hp.hpl.jena.sparql.expr.E_Version;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprList;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.expr.NodeValue;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueNode;


/**
 * In this case, more beautiful means that all expressions, that actually filter
 * are in filter clauses. This means, that ""
 * 
 * @author joerg
 * 
 */
public class QueryNormalizer extends TransformCopy {

	private AlgebraGenerator agen = new  AlgebraGenerator();
	private int i = 0;
	
	private static final Logger LOG = LoggerFactory.getLogger(QueryNormalizer.class); 
	

	Map<String,Node> termToVariable = new HashMap<String, Node>();
	
	TranslationContext context;
	
	
	public QueryNormalizer(TranslationContext context) {
    super();
    this.context = context;
  }

  @Override
	public Op transform(OpQuadPattern quadBlock) {
		List<Quad> patterns = quadBlock.getPattern().getList();
		OpQuadBlock newOp = new OpQuadBlock();
		
		Map<String,String> var2Value = new HashMap<String, String>();
		ExprList exprList = new ExprList();
		List<ExprList> fromFromNamed = Lists.newArrayList();

		for (Quad quad : patterns) {
			quad = uniquefyTriple(quad, exprList); 
			

			newOp.getPattern().add(new Quad(
					rewriteGraphNode(quad.getGraph(), exprList, fromFromNamed ,termToVariable, var2Value),
					rewriteNode(quad.getSubject(), exprList, termToVariable, var2Value), 
					rewriteNode(quad.getPredicate(), exprList, termToVariable, var2Value),
					rewriteNode(quad.getObject(), exprList, termToVariable, var2Value)));
		}

		Op op = newOp;
		if(!exprList.isEmpty()){
		  op = OpFilter.filter(exprList, op);
		} 
		if(!fromFromNamed.isEmpty()){
		  op = recurseFilter(op, fromFromNamed);
		} 
	
		return op;
	}
  
  private Op recurseFilter(Op op, List<ExprList> exprLists){
    Op result = null;
    if(exprLists.size()==0){
      result = op;
    } else if(exprLists.size()==1){
      result = OpFilter.filter(exprLists.get(0), op);
    } else{
      List<ExprList> exprListsMinusOne = exprLists.subList(1,exprLists.size()+1);
      result = OpFilter.filter(exprLists.get(0), recurseFilter(op, exprListsMinusOne));
    }
    
    return result;
  }
	
	private Node rewriteGraphNode(Node graph, ExprList exprList, List<ExprList> fromFromNamed,
      Map<String, Node> termToVariable2, Map<String, String> var2Value) {
	  Node result = null;
	  
	  if(graph.equals(Quad.defaultGraphNodeGenerated)
	       && ! context.getQuery().getGraphURIs().isEmpty() ){
	    Node nNew  = Var.alloc(i++ + ColumnHelper.COL_NAME_INTERNAL);
        termToVariable.put(graph.toString(), nNew);
   
	      List<String> graphuris = context.getQuery().getGraphURIs();
	      ExprList filterIn = createFilterIn(nNew, graphuris);
	      fromFromNamed.add(filterIn);

	     result = nNew; 
	  }else if (graph.isVariable() && !context.getQuery().getNamedGraphURIs().isEmpty()) {
      
	    List<String> graphuris = context.getQuery().getNamedGraphURIs();
      ExprList filterIn = createFilterIn(graph, graphuris);
      fromFromNamed.add(filterIn);
      result = graph;
    }else{
      result = rewriteNode(graph, exprList, termToVariable2, var2Value);
    }
	  
	  return result;
  }

  private ExprList createFilterIn(Node nNew, List<String> graphuris) {
    ExprList filterIn = new ExprList();
    ExprList internal = new ExprList();
    ExprVar nodeVar = new ExprVar(nNew);
    
    for(String graphuri: graphuris){
    
//      internal.add(new ExprVar(nNew));
      internal.add( NodeValueNode.makeNode(NodeFactory.createURI(graphuri)));
      
    }
    filterIn.add(new E_OneOf(nodeVar, internal));
    return filterIn;
  }

  @Override
	public Op transform(OpFilter opFilter, Op subOp) {

		return opFilter.copy(subOp);
	}

	
	/**
	 * Creates a new quad out of the old one, such that
	 * every variable is used only once in the pattern.
	 * 
	 * for example {?x ?x ?y} -> {?x ?genvar_1 ?y. FILTER (?x = ?genvar_1)}
	 * 
	 * @param quad
	 * @param exprList the list with the equals conditions
	 * @return the rewritten Quad
	 */
	private Quad uniquefyTriple(Quad quad, ExprList exprList) {
		
		List<Node> quadNodes = Arrays.asList(quad.getGraph(),quad.getSubject(),quad.getPredicate(),quad.getObject());
		
		List<Node> uniqeNodes = new ArrayList<Node>();
		
		for(Node quadNode: quadNodes){
			if(quadNode.isVariable()&&uniqeNodes.contains(quadNode)){
				Var var_new = Var.alloc(i++ + ColumnHelper.COL_NAME_INTERNAL);
				uniqeNodes.add(var_new);
				exprList.add(new E_Equals(new ExprVar(quadNode),new ExprVar(var_new)));

			}else{
				uniqeNodes.add(quadNode);
			}
		}
		
		return new Quad(uniqeNodes.remove(0), uniqeNodes.remove(0),uniqeNodes.remove(0),uniqeNodes.remove(0));
	}



	private Node rewriteNode(Node n, ExprList addTo, Map<String,Node> termToVariable,Map<String,String> var2Value){

		if(n.isConcrete()){

			Node nNew = termToVariable.get(n.toString()); 
			if(nNew==null){
				nNew = Var.alloc(i++ + ColumnHelper.COL_NAME_INTERNAL);
				termToVariable.put(n.toString(), nNew);
			}


			if(! (var2Value.containsKey(nNew.getName())&&var2Value.get(nNew.getName()).equals(n.toString()))){
				var2Value.put(nNew.getName(), n.toString());

				Expr newExpr = null;
				if(n.isLiteral()){
					newExpr = 
							new E_Equals(new ExprVar(nNew),NodeValue.makeString(n.getLiteralValue().toString()));


					if(n.getLiteralDatatypeURI()!=null && !n.getLiteralDatatypeURI().isEmpty()){
						newExpr = 
								new E_Equals(
										new E_Datatype(new ExprVar(nNew)),
										NodeValue.makeNodeString(n.getLiteralDatatypeURI()));
					}
					if(n.getLiteralLanguage()!=null && !n.getLiteralLanguage().isEmpty()){
						newExpr = new E_LangMatches(new ExprVar(nNew), NodeValue.makeString(n.getLiteralLanguage()));
					}

				}else{
					// no it is not, create the equivalnce check
					newExpr = new E_Equals(new ExprVar(nNew),
							NodeValue.makeNode(n));

				}
				addTo.add(newExpr);
			}	
			n = nNew;
		}
		return n;
	}
	
	@Override
	public Op transform(OpProject opProject, Op subOp) {
		
		
		
		OpVars.mentionedVars(subOp);
		
		
		opProject.getVars();
		// TODO Auto-generated method stub
		return super.transform(opProject, subOp);
	}
	
	
	
	/**
	 * Transforms the query into the form required for sparqlmap.
	 * Includes filter extraction and rewriting some patterns. 
	 * 
	 * @param sparql
	 * @return
	 */
	
	public Op compileToBeauty(Query sparql){
		
		Op query  = agen.compile(sparql);
		
		
		
		
		// this odd construct is neccessary as there seems to be no convenient way of extracting the vars of a query from the non-algebra-version.
		if(sparql.getProject().isEmpty()){
		
			sparql.setQueryResultStar(false);
			List<Var> vars = new ArrayList<Var>( OpVars.mentionedVars(query));
			for (Var var : vars) {
				sparql.getProject().add(var);

			}
			query  = agen.compile(sparql);
		}
		
		query = AlgebraQuad.quadize(query);		
		Op newOp = Transformer.transform(this, query);
		
		
			
		return newOp;
	}
	
	
	
	
	
	
	
}
