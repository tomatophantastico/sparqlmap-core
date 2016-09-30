package org.aksw.sparqlmap.core.mapper;

import java.util.Iterator;

import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.util.deparser.SelectDeParser;

import org.aksw.sparqlmap.core.TranslationContext;
import org.aksw.sparqlmap.core.TranslationContextJDBC;
import org.aksw.sparqlmap.core.db.DBAccess;
import org.aksw.sparqlmap.core.r2rml.jdbc.JDBCTermMapBinder;
import org.aksw.sparqlmap.core.translate.jdbc.DataTypeHelper;
import org.aksw.sparqlmap.core.translate.jdbc.ExpressionConverter;
import org.aksw.sparqlmap.core.translate.jdbc.FilterUtil;
import org.aksw.sparqlmap.core.translate.jdbc.QueryBuilderVisitor;

public class JDBCTranslator {
  
  private DataTypeHelper dth;
  private ExpressionConverter exprconv;
  private FilterUtil filterUtil;
  private JDBCTermMapBinder binder;
  private DBAccess dbaccess;
  
  
  public void translate(TranslationContextJDBC jcontext){
    TranslationContext context = jcontext.getContext();
    
  QueryBuilderVisitor builderVisitor = new QueryBuilderVisitor(context,dth,exprconv,filterUtil,binder);
    
    
    
    
    RightFirstWalker.walk(context.getQueryInformation().getQuery(), builderVisitor);
    
    
    // prepare deparse select
    StringBuilder out = new StringBuilder();
    Select select = builderVisitor.getSqlQuery();
    SelectDeParser selectDeParser  = dbaccess.getSelectDeParser(out);
    
    selectDeParser.setBuffer(out);
//    ExpressionDeParser expressionDeParser =  mappingConf.getR2rconf().getDbConn().getExpressionDeParser(selectDeParser, out);
//    selectDeParser.setExpressionVisitor(expressionDeParser);
    if (select.getWithItemsList() != null && !select.getWithItemsList().isEmpty()) {
      out.append("WITH ");
      for (Iterator iter = select.getWithItemsList().iterator(); iter.hasNext();) {
        WithItem withItem = (WithItem)iter.next();
        out.append(withItem);
        if (iter.hasNext())
          out.append(",");
        out.append(" ");
      }
    }
    select.getSelectBody().accept(selectDeParser);
    
    jcontext.setSqlQuery( out.toString());
    
  }
  

}
