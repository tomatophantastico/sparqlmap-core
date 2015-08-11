package org.aksw.sparqlmap.core.mapper.compatibility;

import net.sf.jsqlparser.schema.Column;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.metamodel.DataContext;
import org.apache.metamodel.schema.ColumnType;

public class CompatibilityCheckerSchemaAware extends
    CompatibilityCheckerSyntactical {
  
  private DataContext context;

  public CompatibilityCheckerSchemaAware(DataContext context) {
    super();
    this.context = context;

  }
  
  /**
   * Examines a column, if it is able to produces certain values.
   * 
   * If the column may not produce the value, false is returned;
   * 
   * @param col
   * @param value
   * @return
   */
  public boolean isCompatible(Column col, String value){
    
    boolean isCompatible = true;
    
    org.apache.metamodel.schema.Table contextTab;
    
    if(col.getTable().getSchemaName()!=null || !col.getTable().getSchemaName().isEmpty()){
      contextTab = context.getDefaultSchema().getTableByName(col.getTable().getName());
    }else{
      contextTab = context.getTableByQualifiedLabel(col.getWholeColumnName());
    }
    
    ColumnType colType =  contextTab.getColumnByName(col.getColumnName()).getType();
    
    if(colType.isNumber() && NumberUtils.isDigits(value)){
      isCompatible = false;
    }
    
    
    return isCompatible;
    
  }

}
