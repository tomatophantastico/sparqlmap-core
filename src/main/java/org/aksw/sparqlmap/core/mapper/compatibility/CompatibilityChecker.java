package org.aksw.sparqlmap.core.mapper.compatibility;

import java.util.Collection;

import org.aksw.sparqlmap.core.r2rml.JDBCTermMap;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.expr.Expr;

public interface CompatibilityChecker {
	
	
	
	boolean isCompatible(Node n);
	
	boolean isCompatible(JDBCTermMap tm);

	boolean isCompatible(String var, Collection<Expr> oxprs);

}
