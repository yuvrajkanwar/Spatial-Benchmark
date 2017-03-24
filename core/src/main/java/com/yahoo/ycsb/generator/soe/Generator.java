package com.yahoo.ycsb.generator.soe;

import com.yahoo.ycsb.workloads.soe.SoeQueryPredicate;
import javafx.util.Pair;


import java.util.ArrayList;
import java.util.Random;

/**
 * Created by oleksandr.gyryk on 3/20/17.
 *
 * The storage-based generator is fetching pre-generated values/documents from an internal in-memory database instead
 * of generating new random values on the fly.
 * This approach allows YCSB to operate with real (or real-looking) JSON documents rather then synthetic.
 *
 * It also provides the ability to query rich JSON documents by splitting JSON documents into query predicates
 * (field, value, type, field-value relation, logical operation)
 */
public abstract class Generator {


  private SoeQueryPredicate _soePredicate;
  private ArrayList<SoeQueryPredicate> _soePredicatesSequence;
  private Pair<String, String> _insertDocument;
  private Random rand =  new Random();

  public static final String SOE_DOCUMENT_PREFIX_CUSTOMER = "customer";
  public static final String SOE_DOCUMENT_PREFIX_ORDER = "order";


  public static final String SOE_METAFIELD_DELIMITER = ":::";
  public static final String SOE_METAFIELD_DOCID  = "SOE_doc_id";
  public static final String SOE_METAFIELD_INSERTDOC = "SOE_insert_document";

  public static final String SOE_FIELD_CUSTOMER_ID  = "_id";
  public static final String SOE_FIELD_CUSTOMER_DOCID = "doc_id";
  public static final String SOE_FIELD_CUSTOMER_GID = "gid";
  public static final String SOE_FIELD_CUSTOMER_FNAME = "first_name";
  public static final String SOE_FIELD_CUSTOMER_LNAME = "middle_name";
  public static final String SOE_FIELD_CUSTOMER_MNAME = "last_name";
  public static final String SOE_FIELD_CUSTOMER_BALLANCE = "ballance_current";
  public static final String SOE_FIELD_CUSTOMER_DOB = "dob";
  public static final String SOE_FIELD_CUSTOMER_EMAIL = "email";
  public static final String SOE_FIELD_CUSTOMER_ISACTIVE = "isActive";
  public static final String SOE_FIELD_CUSTOMER_LINEARSCORE = "linear_score";
  public static final String SOE_FIELD_CUSTOMER_WEIGHTEDSCORE = "weighted_score";
  public static final String SOE_FIELD_CUSTOMER_PHONECOUNTRY = "phone_country";
  public static final String SOE_FIELD_CUSTOMER_PHONE = "phone_by_country";
  public static final String SOE_FIELD_CUSTOMER_AGEGROUP = "age_group";
  public static final String SOE_FIELD_CUSTOMER_AGE = "age_by_group";
  public static final String SOE_FIELD_CUSTOMER_URLPROTOCOL = "url_protocol";
  public static final String SOE_FIELD_CUSTOMER_URLSITE = "url_site";
  public static final String SOE_FIELD_CUSTOMER_URLDOMAIN = "url_domain";
  public static final String SOE_FIELD_CUSTOMER_URL = "url";
  public static final String SOE_FIELD_CUSTOMER_DEVICES = "devices";
  public static final String SOE_FIELD_CUSTOMER_LINKEDDEVICES = "linked_devices";
  public static final String SOE_FIELD_CUSTOMER_ADDRESS = "address";
  public static final String SOE_FIELD_CUSTOMER_ADDRESS_OBJ_STREET = "street";
  public static final String SOE_FIELD_CUSTOMER_ADDRESS_OBJ_CITY = "city";
  public static final String SOE_FIELD_CUSTOMER_ADDRESS_OBJ_ZIP = "zip";
  public static final String SOE_FIELD_CUSTOMER_ADDRESS_OBJ_COUNTRY = "country";
  public static final String SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR = "prev_address";
  public static final String SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_STREET = "street";
  public static final String SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_CITY = "city";
  public static final String SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_COUNTRY = "country";
  public static final String SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_ZIP = "zip";
  public static final String SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_CURRENTOWNER = "property_current_owner";
  public static final String SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_CURRENTOWNER_OBJ_FNAME = "first_name";
  public static final String SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_CURRENTOWNER_OBJ_MNAME = "middle_name";
  public static final String SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_CURRENTOWNER_OBJ_LNAME = "last_name";
  public static final String SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_CURRENTOWNER_OBJ_PHONE = "phone";
  public static final String SOE_FIELD_CUSTOMER_CHILDREN = "children";
  public static final String SOE_FIELD_CUSTOMER_CHILDREN_OBJ_FNAME = "first_name";
  public static final String SOE_FIELD_CUSTOMER_CHILDREN_OBJ_GENDER = "gender";
  public static final String SOE_FIELD_CUSTOMER_CHILDREN_OBJ_AGE = "age";
  public static final String SOE_FIELD_CUSTOMER_VISITEDPLACES = "visited_places";
  public static final String SOE_FIELD_CUSTOMER_VISITEDPLACES_OBJ_COUNTRY = "country";
  public static final String SOE_FIELD_CUSTOMER_VISITEDPLACES_OBJ_CITIES = "cities";

  public static final String SOE_FIELD_ORDER_LIST  = "order_list";



  protected abstract String setVal(String key);
  protected abstract String getVal(String key);
  protected abstract int incremetInsertCounter();



  public Pair<String, String> getInserDocument() {
    return _insertDocument;
  }

  public SoeQueryPredicate getPredicate(){
    return _soePredicate;
  }

  public ArrayList<SoeQueryPredicate> getPredicatesSequence(){
    return _soePredicatesSequence;
  }


  public void buildInsertDocument(int range) {
    String docBody = getVal(_buildKey(SOE_DOCUMENT_PREFIX_CUSTOMER, SOE_METAFIELD_INSERTDOC, range));
    int docCounter = incremetInsertCounter();
    String docKey = SOE_DOCUMENT_PREFIX_CUSTOMER + SOE_METAFIELD_DELIMITER + docCounter;
    _insertDocument = new Pair<String, String>(docKey, docBody);
  }


  public void buildUpdatePredicate(int range) {
    _soePredicate = new SoeQueryPredicate();
    _soePredicate.name = SOE_FIELD_CUSTOMER_BALLANCE;
    _soePredicate.valueA = "$" + rand.nextInt(99999) + "." + rand.nextInt(99);
    _soePredicate.docid = getVal(_buildKey(SOE_DOCUMENT_PREFIX_CUSTOMER, SOE_METAFIELD_DOCID, range));
  }

  public void buildReadPredicate(int range) {
    _soePredicate = new SoeQueryPredicate();
    _soePredicate.docid = getVal(_buildKey(SOE_DOCUMENT_PREFIX_CUSTOMER, SOE_METAFIELD_DOCID, range));
  }

  public void buildScanPredicate(int range) {
    _soePredicate = new SoeQueryPredicate();
    _soePredicate.docid = getVal(_buildKey(SOE_DOCUMENT_PREFIX_CUSTOMER, SOE_METAFIELD_DOCID, range));
  }


  public void buildPagePredicate(int range) {
    _soePredicate.name = SOE_FIELD_CUSTOMER_ADDRESS;
    _soePredicate.nestedPredicateA = new SoeQueryPredicate();
    _soePredicate.nestedPredicateA.name = SOE_FIELD_CUSTOMER_ADDRESS_OBJ_ZIP;
    _soePredicate.nestedPredicateA.valueA = getVal(_buildKey(SOE_DOCUMENT_PREFIX_CUSTOMER,
                                                             SOE_FIELD_CUSTOMER_ADDRESS,
                                                             SOE_FIELD_CUSTOMER_ADDRESS_OBJ_ZIP,
                                                             range));
    _soePredicate.nestedPredicateA.type = SoeQueryPredicate.SOE_PREDICATE_TYPE_INTEGER;
  }



  public void buildSearchPredicatesSequenceN3(int range) {
    SoeQueryPredicate predicate;

    String key1 = SOE_DOCUMENT_PREFIX_CUSTOMER + SOE_METAFIELD_DELIMITER +
        SOE_FIELD_CUSTOMER_ADDRESS + SOE_METAFIELD_DELIMITER +
        SOE_FIELD_CUSTOMER_ADDRESS_OBJ_COUNTRY + SOE_METAFIELD_DELIMITER + rand.nextInt(range);

    String key2 = SOE_DOCUMENT_PREFIX_CUSTOMER + SOE_METAFIELD_DELIMITER
        + SOE_FIELD_CUSTOMER_AGEGROUP + SOE_METAFIELD_DELIMITER + rand.nextInt(range);

    String key3 = SOE_DOCUMENT_PREFIX_CUSTOMER + SOE_METAFIELD_DELIMITER
        + SOE_FIELD_CUSTOMER_DOB + SOE_METAFIELD_DELIMITER + rand.nextInt(range);


    predicate = new SoeQueryPredicate();
    predicate.name = SOE_FIELD_CUSTOMER_ADDRESS;
    predicate.nestedPredicateA = new SoeQueryPredicate();
    predicate.nestedPredicateA.name = SOE_FIELD_CUSTOMER_ADDRESS_OBJ_COUNTRY;
    predicate.nestedPredicateA.valueA = getVal(_buildKey(SOE_DOCUMENT_PREFIX_CUSTOMER,
                                                         SOE_FIELD_CUSTOMER_ADDRESS,
                                                         SOE_FIELD_CUSTOMER_ADDRESS_OBJ_COUNTRY, range));
    _soePredicatesSequence.add(predicate);


    predicate = new SoeQueryPredicate();
    predicate.name = SOE_FIELD_CUSTOMER_AGEGROUP;
    predicate.valueA = getVal(_buildKey(SOE_DOCUMENT_PREFIX_CUSTOMER, SOE_FIELD_CUSTOMER_AGEGROUP, range));
    _soePredicatesSequence.add(predicate);


    predicate = new SoeQueryPredicate();
    predicate.name = SOE_FIELD_CUSTOMER_DOB;
    predicate.valueA = getVal(_buildKey(SOE_DOCUMENT_PREFIX_CUSTOMER, SOE_FIELD_CUSTOMER_DOB, range));
    _soePredicatesSequence.add(predicate);

  }

  public void buildNestedScanPredicate(int range) {
    SoeQueryPredicate predicate = new SoeQueryPredicate();
    predicate.name = SOE_FIELD_CUSTOMER_ADDRESS;
    predicate.nestedPredicateA = new SoeQueryPredicate();
    predicate.nestedPredicateA.name = SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR;
    predicate.nestedPredicateA.nestedPredicateA = new SoeQueryPredicate();
    predicate.nestedPredicateA.nestedPredicateA.name = SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_ZIP;
    String key = _buildKey(SOE_DOCUMENT_PREFIX_CUSTOMER,
                           SOE_FIELD_CUSTOMER_ADDRESS,
                           SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR,
                           SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_ZIP, range);
    predicate.nestedPredicateA.nestedPredicateA.valueA = getVal(key);
    predicate.nestedPredicateA.nestedPredicateA.type = SoeQueryPredicate.SOE_PREDICATE_TYPE_INTEGER;
  }


  public void buildArrayScan1Predicate(int range) {
    SoeQueryPredicate predicate = new SoeQueryPredicate();
    predicate.name = SOE_FIELD_CUSTOMER_DEVICES;
    String key = SOE_DOCUMENT_PREFIX_CUSTOMER + SOE_METAFIELD_DELIMITER +
                SOE_FIELD_CUSTOMER_DEVICES + SOE_METAFIELD_DELIMITER +rand.nextInt(range);

    predicate.valueA = getVal(_buildKey(SOE_DOCUMENT_PREFIX_CUSTOMER, SOE_FIELD_CUSTOMER_DEVICES, range));
  }


  public void buildArrayScan2Predicate(int range) {
    SoeQueryPredicate predicate = new SoeQueryPredicate();
    predicate.name = SOE_FIELD_CUSTOMER_VISITEDPLACES;
    predicate.nestedPredicateA = new SoeQueryPredicate();
    predicate.nestedPredicateA.name = SOE_FIELD_CUSTOMER_VISITEDPLACES_OBJ_COUNTRY;
    predicate.nestedPredicateA.valueA = getVal(_buildKey(SOE_DOCUMENT_PREFIX_CUSTOMER,
                                                         SOE_FIELD_CUSTOMER_VISITEDPLACES,
                                                         SOE_FIELD_CUSTOMER_VISITEDPLACES_OBJ_COUNTRY, range));
    predicate.nestedPredicateB = new SoeQueryPredicate();
    predicate.nestedPredicateB.name = SOE_FIELD_CUSTOMER_VISITEDPLACES_OBJ_COUNTRY;
    predicate.nestedPredicateB.valueA = getVal(_buildKey(SOE_DOCUMENT_PREFIX_CUSTOMER,
                                                         SOE_FIELD_CUSTOMER_VISITEDPLACES,
                                                         SOE_FIELD_CUSTOMER_VISITEDPLACES_OBJ_CITIES, range));
  }


  public void buildReport1Predicate() {
    // todo
  }


  public void buildReport2Predicate() {
    // todo
  }


  private String _buildKey(String token1, int range) {
    return token1 + SOE_METAFIELD_DELIMITER  + rand.nextInt(range);
  }

  private String _buildKey(String token1, String token2, int range) {
    return token1 + SOE_METAFIELD_DELIMITER + _buildKey(token2, range);
  }

  private String _buildKey(String token1, String token2, String token3, int range) {
    return token1 + SOE_METAFIELD_DELIMITER + _buildKey(token2, token3, range);
  }

  private String _buildKey(String token1, String token2, String token3, String token4, int range) {
    return token1 + SOE_METAFIELD_DELIMITER + _buildKey(token2, token3, token4, range);
  }

  private String _buildKey(String token1, String token2, String token3, String token4, String token5, int range) {
    return token1 + SOE_METAFIELD_DELIMITER + _buildKey(token2, token3, token4, token5, range);
  }


}
