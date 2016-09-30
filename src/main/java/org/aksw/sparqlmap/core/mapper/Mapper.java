package org.aksw.sparqlmap.core.mapper;

import org.aksw.sparqlmap.core.TranslationContext;
import org.aksw.sparqlmap.core.db.DBAccess;
import org.aksw.sparqlmap.core.mapper.finder.QueryInformation;
import org.aksw.sparqlmap.core.r2rml.jdbc.JDBCColumnHelper;
import org.aksw.sparqlmap.core.r2rml.jdbc.JDBCMapping;
import org.aksw.sparqlmap.core.r2rml.jdbc.JDBCTermMapBinder;
import org.aksw.sparqlmap.core.translate.jdbc.DataTypeHelper;
import org.aksw.sparqlmap.core.translate.jdbc.ExpressionConverter;
import org.aksw.sparqlmap.core.translate.jdbc.FilterUtil;
import org.aksw.sparqlmap.core.translate.jdbc.JDBCOptimizationConfiguration;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

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
