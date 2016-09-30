package org.aksw.sparqlmap.core.mapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.util.deparser.SelectDeParser;

import org.aksw.sparqlmap.core.TranslationContext;
import org.aksw.sparqlmap.core.db.DBAccess;
import org.aksw.sparqlmap.core.mapper.finder.Binder;
import org.aksw.sparqlmap.core.mapper.finder.FilterFinder;
import org.aksw.sparqlmap.core.mapper.finder.MappingBinding;
import org.aksw.sparqlmap.core.mapper.finder.QueryInformation;
import org.aksw.sparqlmap.core.normalizer.QueryNormalizer;
import org.aksw.sparqlmap.core.normalizer.RenameExtractVisitor;
import org.aksw.sparqlmap.core.r2rml.jdbc.JDBCColumnHelper;
import org.aksw.sparqlmap.core.r2rml.jdbc.JDBCMapping;
import org.aksw.sparqlmap.core.r2rml.jdbc.JDBCQuadMap;
import org.aksw.sparqlmap.core.r2rml.jdbc.JDBCTermMapBinder;
import org.aksw.sparqlmap.core.translate.jdbc.DataTypeHelper;
import org.aksw.sparqlmap.core.translate.jdbc.ExpressionConverter;
import org.aksw.sparqlmap.core.translate.jdbc.FilterUtil;
import org.aksw.sparqlmap.core.translate.jdbc.JDBCOptimizationConfiguration;
import org.aksw.sparqlmap.core.translate.jdbc.QueryBuilderVisitor;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.AlgebraGenerator;
import org.apache.jena.sparql.algebra.AlgebraQuad;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpWalker;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.algebra.op.OpQuadPattern;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;

@Service
public class Mapper {
	
	static Logger log = org.slf4j.LoggerFactory.getLogger(Mapper.class);

	@Autowired
	private JDBCMapping mappingConf;

	@Autowired
	private DBAccess dbconf;
	
	@Autowired
	private DataTypeHelper dth;
	
	@Autowired
	private JDBCColumnHelper colhelp;
	
	@Autowired 
	private ExpressionConverter exprconv;
	
	@Autowired
	private FilterUtil filterUtil;
	
	@Autowired
	private JDBCOptimizationConfiguration fopt;
	
	@Autowired
	private JDBCTermMapBinder tmf;
	

	
	


	public void rewrite(TranslationContext context) {
		
	
		
		if(fopt.isOptimizeProjectPush()){
			QueryPushOptimization.setProjectionPush(context);
		}
		
		
		if(fopt.isOptimizeSelfUnion()){
			
		
			QueryDeunifier unionOpt = new QueryDeunifier(context.getQueryInformation(), context.getQueryBinding(),dth,exprconv,colhelp,fopt);
			if(!unionOpt.isFailed()){
			QueryInformation newqi = unionOpt.getQueryInformation();
			newqi.setProjectionPushable(context.getQueryInformation().isProjectionPush());
			context.setQueryInformation(newqi);
			context.setQueryBinding( unionOpt.getQueryBinding());
			}
		}
		
		
		// Backend unspecific processing ends here, 
		
	
		
	}
}
