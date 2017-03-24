package com.yahoo.ycsb.workloads.soe;

/**
 * Created by oleksandr.gyryk on 3/20/17.
 */
public class SoeQueryPredicate {

  public static final String SOE_PREDICATE_TYPE_STRING = "string";
  public static final String SOE_PREDICATE_TYPE_INTEGER = "int";
  public static final String SOE_PREDICATE_TYPE_BOOLEAN = "bool";


  public String name = "";
  public String valueA = "";
  public String valueB = "";
  public String docid = "";
  public String operation = "";
  public String relation = "";
  public String type = SOE_PREDICATE_TYPE_STRING;


  public SoeQueryPredicate nestedPredicateA = null;
  public SoeQueryPredicate nestedPredicateB = null;
  public SoeQueryPredicate nestedPredicateC = null;
  public SoeQueryPredicate nestedPredicateD = null;


}
