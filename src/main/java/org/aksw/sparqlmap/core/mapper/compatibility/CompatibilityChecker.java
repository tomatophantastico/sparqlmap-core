package org.aksw.sparqlmap.core.mapper.compatibility;

import java.util.Collection;

import org.aksw.sparqlmap.core.r2rml.JDBCTermMap;
import org.aksw.sparqlmap.core.r2rml.TermMap;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.expr.Expr;

public interface CompatibilityChecker {
	
	
	boolean isCompatible(TermMap tm1, TermMap tm2 );

	boolean isCompatible(TermMap tm, String var, Collection<Expr> oxprs);

}
