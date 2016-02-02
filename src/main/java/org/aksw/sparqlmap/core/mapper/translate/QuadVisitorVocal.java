package org.aksw.sparqlmap.core.mapper.translate;

import org.aksw.sparqlmap.core.ImplementationException;

import com.hp.hpl.jena.sparql.algebra.op.OpAssign;
import com.hp.hpl.jena.sparql.algebra.op.OpConditional;
import com.hp.hpl.jena.sparql.algebra.op.OpDatasetNames;
import com.hp.hpl.jena.sparql.algebra.op.OpDiff;
import com.hp.hpl.jena.sparql.algebra.op.OpDisjunction;
import com.hp.hpl.jena.sparql.algebra.op.OpDistinct;
import com.hp.hpl.jena.sparql.algebra.op.OpExt;
import com.hp.hpl.jena.sparql.algebra.op.OpExtend;
import com.hp.hpl.jena.sparql.algebra.op.OpGroup;
import com.hp.hpl.jena.sparql.algebra.op.OpJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpLabel;
import com.hp.hpl.jena.sparql.algebra.op.OpLeftJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpList;
import com.hp.hpl.jena.sparql.algebra.op.OpMinus;
import com.hp.hpl.jena.sparql.algebra.op.OpNull;
import com.hp.hpl.jena.sparql.algebra.op.OpOrder;
import com.hp.hpl.jena.sparql.algebra.op.OpPath;
import com.hp.hpl.jena.sparql.algebra.op.OpProcedure;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.algebra.op.OpPropFunc;
import com.hp.hpl.jena.sparql.algebra.op.OpQuad;
import com.hp.hpl.jena.sparql.algebra.op.OpQuadPattern;
import com.hp.hpl.jena.sparql.algebra.op.OpReduced;
import com.hp.hpl.jena.sparql.algebra.op.OpSequence;
import com.hp.hpl.jena.sparql.algebra.op.OpService;
import com.hp.hpl.jena.sparql.algebra.op.OpSlice;
import com.hp.hpl.jena.sparql.algebra.op.OpTable;
import com.hp.hpl.jena.sparql.algebra.op.OpTopN;
import com.hp.hpl.jena.sparql.algebra.op.OpTriple;
import com.hp.hpl.jena.sparql.algebra.op.OpUnion;


/**
 * This Visitor throws exceptions for not overriden methods
 * 
 * @author joerg
 *
 */
public class QuadVisitorVocal extends QuadVisitorBase {





  @Override
  public void visit(OpPath opPath) {
    throw new ImplementationException("Not Implemeted yet");

  }

  @Override
  public void visit(OpProcedure opProc) {
    throw new ImplementationException("Not Implemeted yet");

  }

  @Override
  public void visit(OpPropFunc opPropFunc) {
    throw new ImplementationException("Not Implemeted yet");

  }

  @Override
  public void visit(OpJoin opJoin) {
    throw new ImplementationException("Not Implemeted yet");

  }

  @Override
  public void visit(OpSequence opSequence) {
    throw new ImplementationException("Not Implemeted yet");

  }

  @Override
  public void visit(OpDisjunction opDisjunction) {
    throw new ImplementationException("Not Implemeted yet");

  }

  @Override
  public void visit(OpLeftJoin opLeftJoin) {
    throw new ImplementationException("Not Implemeted yet");

  }

  @Override
  public void visit(OpConditional opCond) {
    throw new ImplementationException("Not Implemeted yet");

  }

  @Override
  public void visit(OpMinus opMinus) {
    throw new ImplementationException("Not Implemeted yet");

  }

  @Override
  public void visit(OpDiff opDiff) {
    throw new ImplementationException("Not Implemeted yet");

  }



  @Override
  public void visit(OpService opService) {
    throw new ImplementationException("Not Implemeted yet");

  }

  @Override
  public void visit(OpDatasetNames dsNames) {
    throw new ImplementationException("Not Implemeted yet");

  }



  @Override
  public void visit(OpExt opExt) {
    throw new ImplementationException("Not Implemeted yet");

  }

  @Override
  public void visit(OpNull opNull) {
    throw new ImplementationException("Not Implemeted yet");

  }

  @Override
  public void visit(OpLabel opLabel) {
    throw new ImplementationException("Not Implemeted yet");

  }

  @Override
  public void visit(OpAssign opAssign) {
    throw new ImplementationException("Not Implemeted yet");

  }


  @Override
  public void visit(OpList opList) {
    throw new ImplementationException("Not Implemeted yet");

  }

  @Override
  public void visit(OpOrder opOrder) {
    throw new ImplementationException("Not Implemeted yet");

  }





  @Override
  public void visit(OpReduced opReduced) {
    throw new ImplementationException("Not Implemeted yet");

  }

  @Override
  public void visit(OpSlice opSlice) {
   throw new ImplementationException("Not Implemeted yet");
  }

  @Override
  public void visit(OpGroup opGroup) {
    throw new ImplementationException("Not Implemeted yet");

  }

  @Override
  public void visit(OpTopN opTop) {
    throw new ImplementationException("Not Implemeted yet");

  }
  
  
  

}
