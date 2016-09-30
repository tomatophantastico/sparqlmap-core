package org.aksw.sparqlmap.core.r2rml.jdbc;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import org.aksw.sparqlmap.core.ImplementationException;
import org.aksw.sparqlmap.core.db.DBAccess;
import org.aksw.sparqlmap.core.r2rml.BoundQuadMap;
import org.aksw.sparqlmap.core.r2rml.QuadMap.LogicalTable;
import org.aksw.sparqlmap.core.r2rml.R2RML;
import org.aksw.sparqlmap.core.r2rml.TermMap;
import org.aksw.sparqlmap.core.r2rml.TermMapColumn;
import org.aksw.sparqlmap.core.r2rml.TermMapConstant;
import org.aksw.sparqlmap.core.r2rml.TermMapReferencing;
import org.aksw.sparqlmap.core.r2rml.TermMapReferencing.JoinOn;
import org.aksw.sparqlmap.core.r2rml.TermMapTemplate;
import org.aksw.sparqlmap.core.r2rml.TermMapTemplateTuple;
import org.aksw.sparqlmap.core.translate.jdbc.DataTypeHelper;
import org.aksw.sparqlmap.core.translate.jdbc.FilterUtil;
import org.apache.jena.datatypes.BaseDatatype;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.datatypes.xsd.XSDDateTime;
import org.apache.jena.graph.impl.LiteralLabelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.RDFS;

import com.google.common.collect.Lists;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.FromItem;

/**
 * This factory creates JDBC TermMaps, i.e. TermMaps that are bound to a
 * specific database.
 * 
 * @author joerg
 *
 */
public class JDBCTermMapBinder {

  private DataTypeHelper dth;
  private DBAccess dbaccess;
  private Map<LogicalTable, FromItem> fromItems;

  public JDBCTermMapBinder(DBAccess dbaccess, Map<LogicalTable, FromItem> fromItems) {
    super();
    this.dth = dbaccess.getDataTypeHelper();
    this.dbaccess = dbaccess;
    this.fromItems = fromItems;
  }

  /**
   * create a TermMap for a static node.
   * 
   * @param node
   * @return
   */
  public JDBCTermMap createTermMap(TermMapConstant constant) {

    JDBCTermMap tm = new JDBCTermMap(dth);
    


    if (constant.getConstantLiteral() != null) {
      tm.setTermTyp(R2RML.LITERAL);

      String constLit = constant.getConstantLiteral();


      RDFDatatype datatype = new BaseDatatype(RDFS.Literal.getURI());
      if (constant.getDatatypIRI() != null) {
        datatype = new BaseDatatype(constant.getDatatypIRI());
      }

      // set the value here
      if (dth.getCastTypeString(datatype).equals(dth.getStringCastType())) {
        StringValue stringVal = new StringValue("'" + constLit + "'");
        tm.literalValString = dth.cast(stringVal, dth.getStringCastType());

      } else if (dth.getCastTypeString(datatype).equals(dth.getNumericCastType())) {
        LongValue longValue = new LongValue(constLit);
        tm.literalValNumeric = dth.cast(longValue, dth.getNumericCastType());

      } else if (dth.getCastTypeString(datatype).equals(dth.getBinaryDataType())) {
        StringValue binVal = new StringValue("'" + constLit + "'");
        tm.literalValBinary = dth.cast(binVal, dth.getBinaryDataType());

      } else if (dth.getCastTypeString(datatype).equals(dth.getDateCastType())) {
        Long timestamp;
        Object value = LiteralLabelFactory.createLiteralLabel(constLit, null, datatype);
        if (value instanceof XSDDateTime) {

          timestamp = ((XSDDateTime) value).asCalendar().getTimeInMillis();
        } else {
          throw new ImplementationException("Encountered unkown datatype as data:" + value.getClass());
        }

        TimestampValue dateValue = new TimestampValue(new Timestamp(timestamp));
        tm.literalValDate = dth.cast(dateValue, dth.getDateCastType());

      } else if (dth.getCastTypeString(datatype).equals(dth.getBooleanCastType())) {
        StringValue bool = new StringValue("'" + constLit + "'");
        tm.literalValBool = dth.cast(bool, dth.getBooleanCastType());
      }

    } else {
      // not a Literal, so it has to be a resource
      tm.getResourceColSeg().add(resourceToExpression(constant.getConstantIRI()));

      if (constant.getTermTypeIRI().equals(R2RML.BLANKNODE_STRING)) {
        tm.setTermTyp(R2RML.BLANKNODE);
      } else {
        tm.setTermTyp(R2RML.IRI);
      }
    }

    return tm;
  }

  public JDBCTermMap createBoolTermMap(Expression bool) {
    JDBCTermMap tm = new JDBCTermMap(dth);
    tm.setTermTyp(R2RML.LITERAL);
    tm.setLiteralDataType(XSDDatatype.XSDboolean.getURI());
    tm.literalValBool = dth.cast(bool, dth.getBooleanCastType());

    return tm;
  }
  
 
  public JDBCTermMap createStringTermMap(Expression stringExpression,RDFDatatype datatype, Expression lang) {
    JDBCTermMap tm = new JDBCTermMap(dth);
    tm.setTermTyp(R2RML.LITERAL);
    
    if(datatype==null){
      tm.setLiteralDataType(RDFS.Literal.getURI());
    }else{
      tm.setLiteralDataType(datatype.getURI());
    }
    
    if(lang!=null){
      tm.setLiteralLang(lang);
    }
    
    tm.literalValString = dth.cast(stringExpression, dth.getStringCastType());
    return tm;
  }

  public JDBCTermMap createNumericalTermMap(Expression numeric, Expression datatype) {
    JDBCTermMap tm = new JDBCTermMap(dth);
    tm.setTermTyp(R2RML.LITERAL);
    tm.literalType = datatype;
    tm.literalValNumeric = dth.cast(numeric, dth.getNumericCastType());

    return tm;
  }

  public JDBCTermMap createNumericalTermMap(Expression numeric, XSDDatatype datatype) {
    JDBCTermMap tm = new JDBCTermMap(dth);
    tm.setTermTyp(R2RML.LITERAL);
    tm.setLiteralDataType(datatype.getURI());
    tm.literalValNumeric = dth.cast(numeric, dth.getNumericCastType());

    return tm;
  }
  
  
  
  public JDBCTermMap createResourceTermMap(List<Expression> resourceExpressions){
    JDBCTermMap tm = new JDBCTermMap(dth);
    tm.setTermTyp(R2RML.IRI);
    tm.setResourceExpression(resourceExpressions);
    return tm;
    
  }
  

  private Expression resourceToExpression(String uri) {
    return dth.cast(new StringValue("\"" + uri + "\""), dth.getStringCastType());
  }
  


  /**
   * binds {@link TermMap}s to the underlying data context. Ignores {@link TermMapReferencing}
   * 
   * @param termMap
   * @param jQuadMap
   * @return
   */
  public JDBCTermMap bind(TermMap termMap, FromItem fromItem) {
    
    JDBCTermMap jTermMap = null;

    
    Expression lang = getLang(termMap);
    RDFDatatype datatype = getDataType(termMap);
    
    
    // with constant expressions
    if (termMap instanceof TermMapConstant) {
      jTermMap = createTermMap((TermMapConstant) termMap);
      // deal with template based term maps
    } else if (termMap instanceof TermMapTemplate) {
      // can be either Literal or Resource
      TermMapTemplate termMapTemplate = (TermMapTemplate) termMap;
      //deal with Literal template expressions
      if (termMap.getTermTypeIRI().equals(R2RML.LITERAL_STRING)) {

        List<Expression> resourceExpression = templateToResourceExpression(termMapTemplate.getTemplate(),
            fromItem, dth);
        jTermMap = createStringTermMap(FilterUtil.concat(resourceExpression.toArray(new Expression[0])),
                      new BaseDatatype(termMap.getDatatypIRI()),lang);
        
        
      //deal with resource template expressions
      } else {
        List<Expression> resourceExpression = templateToResourceExpression(((TermMapTemplate) termMap).getTemplate(), fromItem, dth);
        jTermMap.getResourceColSeg().addAll(resourceExpression);
      }

    // has to be column based
    } else if (termMap instanceof TermMapColumn) {
      TermMapColumn termMapCol = (TermMapColumn) termMap;
      Column col = JDBCColumnHelper.createColumn(fromItem.getAlias(), termMapCol.getColumn());
      
      if (termMapCol.getTermTypeIRI().equals(R2RML.LITERAL_STRING)) {
        

        if (datatype == null) {
          // get the default datatype for this sql type
          int sqlType = dbaccess.getDataType(fromItem,col.getColumnName());
          RDFDatatype dt = DataTypeHelper.getRDFDataType(sqlType);

          // set column specific default or rdfs:Literal
        }
        
        jTermMap.setLiteralDataType(datatype.getURI());

        // determine the column, where to put/cast the expression to

        if (dth.getCastTypeString(datatype).equals(dth.getStringCastType())) {
          jTermMap.literalValString = dth.cast(col, dth.getStringCastType());

        } else if (dth.getCastTypeString(datatype).equals(dth.getNumericCastType())) {
          jTermMap.literalValNumeric = dth.cast(col, dth.getNumericCastType());

        } else if (dth.getCastTypeString(datatype).equals(dth.getBinaryDataType())) {
          jTermMap.literalValBinary = dth.cast(col, dth.getBinaryDataType());

        } else if (dth.getCastTypeString(datatype).equals(dth.getDateCastType())) {
          jTermMap.literalValDate = dth.cast(col, dth.getDateCastType());

        } else if (dth.getCastTypeString(datatype).equals(dth.getBooleanCastType())) {
          jTermMap.literalValBool = dth.cast(col, dth.getBooleanCastType());
        }

      } else {
        // tm.resourceColSeg.add(dth.castNull(dth.getStringCastType()));
        jTermMap.setResourceExpression(
            Lists.newArrayList(dth.cast(col, dth.getStringCastType())));
      }

    } else {
      throw new ImplementationException("Unexpected TermMap type");
    }

   

    if (!jTermMap.isConstant()) {
      jTermMap.getAlias2fromItem().put(fromItem.getAlias(), fromItem);
    }
   
    return jTermMap;
  }

  private RDFDatatype getDataType(TermMap termMap) {
    RDFDatatype result = null;
    if(termMap.getDatatypIRI()!=null){
      result = new BaseDatatype(termMap.getDatatypIRI());
    }
    
    return result;
  }

  private Expression getLang(TermMap termMap) {
    Expression lang = null;
    if(termMap.getLang() != null){
      lang = dth.asString(termMap.getLang());
    }
    return lang;
  }

  public Expression resourceToExpression(Resource res) {
    return dth.cast(new StringValue("\"" + res.getURI() + "\""), dth.getStringCastType());
  }

  private List<Expression> templateToResourceExpression(List<TermMapTemplateTuple> list, FromItem fi, DataTypeHelper dth) {

    List<Expression> newExprs = Lists.newArrayList();
    
    // now create a big concat statement.
    for (TermMapTemplateTuple ttsc: list) {
      //String has always to be set
      if(ttsc.getString() ==null){
        throw new ImplementationException("Unexpected null value");
      }
      newExprs.add(dth.cast(new StringValue("\"" + ttsc.getString() + "\""), dth.getStringCastType()));
      //optionally deal with a column
      if(ttsc.getColumn()!=null){
        String colName = ttsc.getColumn();
        newExprs.add(dth.cast(JDBCColumnHelper.createColumn(fi.getAlias(), colName), dth.getStringCastType()));

      }
    }
    return newExprs;

  }

  public JDBCTermMap bindRefMap(TermMap termMap, BoundQuadMap jQuadMap,JDBCMapping jdbcmapping, Map<LogicalTable,FromItem> fromItems ) {
    
    
    TermMapReferencing termMapRef = (TermMapReferencing) termMap;
       
    TermMap parentTermMap =  termMapRef.getParent();
    
    
    JDBCTermMap parentJTermMap = jdbcmapping.getBoundTermMap(parentTermMap);
    
    JDBCTermMap result = parentJTermMap.clone("");
    
    
    if(termMapRef.getConditions().isEmpty()){
    }else{    
      for(JoinOn joinon: termMapRef.getConditions()){
        //at this time, only one from item can be present
        Column leftCol = JDBCColumnHelper.createColumn(
            parentJTermMap.getFromItems().get(0).getAlias(), joinon.getParentColumn());
        
        //get the from item of the referenced term map
        FromItem childFromItem = fromItems.get(termMapRef.getQuadMap().getLogicalTable());
        
        Column rightCol = JDBCColumnHelper.createColumn( childFromItem.getAlias(), joinon.getChildColumn());
        EqualsTo eq = new EqualsTo();
        eq.setLeftExpression(dth.cast(leftCol, dth.getStringCastType()));
        eq.setRightExpression(dth.cast(rightCol, dth.getStringCastType()));
        
        result.addFromItem(childFromItem);
        result.getJoinConditions().add(eq);
        
      }
    }
    
    return result;
   
  }

}
