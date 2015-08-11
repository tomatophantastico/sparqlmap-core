package org.aksw.sparqlmap.core.mapper.compatibility;

import static org.junit.Assert.*;

import org.aksw.sparqlmap.core.db.impl.HSQLDBDataTypeHelper;
import org.aksw.sparqlmap.core.mapper.translate.DataTypeHelper;
import org.aksw.sparqlmap.core.r2rml.TermMap;
import org.aksw.sparqlmap.core.r2rml.TermMapFactory;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.hp.hpl.jena.graph.NodeFactory;

public class CompatibilityTest {
  
  
  static DataTypeHelper dth = new HSQLDBDataTypeHelper();

  @Test
  public void testBasicCompatibilities() {
    
    CompatibilityChecker cchecker = new CompatibilityCheckerSyntactical();
    
    TermMapFactory tmf = new TermMapFactory();
    tmf.setDth(dth);
    
    
    
    TermMap staticUrl1= tmf.createTermMap(NodeFactory.createURI("http://example.com/test/Resource1"));
    TermMap staticUrl2= tmf.createTermMap(NodeFactory.createURI("http://example.com/test/Resource/2"));
    
    TermMap dynamicUrl = tmf.createResourceTermMap(Lists.newArrayList("http://example.com/test/Resource/{x}"))

    assertFalse(cchecker.isCompatible(staticUrl1, staticUrl2));
    
    
  
  }

}
