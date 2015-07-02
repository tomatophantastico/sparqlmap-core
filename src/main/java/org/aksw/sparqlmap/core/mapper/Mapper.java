package org.aksw.sparqlmap.core.mapper;


import java.util.List;

import org.aksw.sparqlmap.core.TranslationContext;

public interface Mapper {


	 /** transforms  a sparql select query into sql
	 * 
	 * @param Sparql
	 * @return
	 */
	public abstract void rewrite(TranslationContext context);
	


	public abstract List<TranslationContext> dump();

}