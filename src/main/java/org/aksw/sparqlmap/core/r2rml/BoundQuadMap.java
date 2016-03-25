package org.aksw.sparqlmap.core.r2rml;

import org.aksw.sparqlmap.core.mapper.compatibility.CompatibilityChecker;

import util.QuadPosition;

public interface BoundQuadMap {
  
  public abstract CompatibilityChecker getCompatibilityChecker(QuadPosition qpos);
  
}