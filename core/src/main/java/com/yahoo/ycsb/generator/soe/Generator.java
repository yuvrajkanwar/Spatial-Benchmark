package com.yahoo.ycsb.generator.soe;

import com.yahoo.ycsb.workloads.soe.SoeQueryPredicate;
import com.yahoo.ycsb.workloads.soe.SoeWorkload;
import javafx.util.Pair;


import java.util.*;

import org.json.*;


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

  private int totalDocsCount = 0;
  private int storedDocsCount = 0;
  private Random rand = new Random();

  private boolean allValuesInitialized = false;
  private Properties properties;
  private int queryLimitMin = 0;
  private int queryLimitMax = 0;
  private int queryOffsetMin = 0;
  private int queryOffsetMax = 0;




  private SoeQueryPredicate soePredicate;
  private ArrayList<SoeQueryPredicate> soePredicatesSequence;
  private Pair<String, String> insertDocument;

  public static final String SOE_DOCUMENT_PREFIX_CUSTOMER = "customer";
  public static final String SOE_DOCUMENT_PREFIX_ORDER = "order";

  public static final String SOE_SYSTEMFIELD_DELIMITER = ":::";
  public static final String SOE_SYSTEMFIELD_INSERTDOC_COUNTER = "SOE_insert_document_counter";
  public static final String SOE_SYSTEMFIELD_STORAGEDOCS_COUNT = "SOE_storage_docs_count";
  public static final String SOE_SYSTEMFIELD_TOTALDOCS_COUNT = "SOE_total_docs_count";

  private static final String SOE_METAFIELD_DOCID = "SOE_doc_id";
  private static final String SOE_METAFIELD_INSERTDOC = "SOE_insert_document";

  private static final String SOE_FIELD_CUSTOMER_ID = "_id";
  private static final String SOE_FIELD_CUSTOMER_DOCID = "doc_id";
  private static final String SOE_FIELD_CUSTOMER_GID = "gid";
  private static final String SOE_FIELD_CUSTOMER_FNAME = "first_name";
  private static final String SOE_FIELD_CUSTOMER_LNAME = "middle_name";
  private static final String SOE_FIELD_CUSTOMER_MNAME = "last_name";
  private static final String SOE_FIELD_CUSTOMER_BALLANCE = "ballance_current";
  private static final String SOE_FIELD_CUSTOMER_DOB = "dob";
  private static final String SOE_FIELD_CUSTOMER_EMAIL = "email";
  private static final String SOE_FIELD_CUSTOMER_ISACTIVE = "isActive";
  private static final String SOE_FIELD_CUSTOMER_LINEARSCORE = "linear_score";
  private static final String SOE_FIELD_CUSTOMER_WEIGHTEDSCORE = "weighted_score";
  private static final String SOE_FIELD_CUSTOMER_PHONECOUNTRY = "phone_country";
  private static final String SOE_FIELD_CUSTOMER_PHONE = "phone_by_country";
  private static final String SOE_FIELD_CUSTOMER_AGEGROUP = "age_group";
  private static final String SOE_FIELD_CUSTOMER_AGE = "age_by_group";
  private static final String SOE_FIELD_CUSTOMER_URLPROTOCOL = "url_protocol";
  private static final String SOE_FIELD_CUSTOMER_URLSITE = "url_site";
  private static final String SOE_FIELD_CUSTOMER_URLDOMAIN = "url_domain";
  private static final String SOE_FIELD_CUSTOMER_URL = "url";
  private static final String SOE_FIELD_CUSTOMER_DEVICES = "devices";
  private static final String SOE_FIELD_CUSTOMER_LINKEDDEVICES = "linked_devices";
  private static final String SOE_FIELD_CUSTOMER_ADDRESS = "address";
  private static final String SOE_FIELD_CUSTOMER_ADDRESS_OBJ_STREET = "street";
  private static final String SOE_FIELD_CUSTOMER_ADDRESS_OBJ_CITY = "city";
  private static final String SOE_FIELD_CUSTOMER_ADDRESS_OBJ_ZIP = "zip";
  private static final String SOE_FIELD_CUSTOMER_ADDRESS_OBJ_COUNTRY = "country";
  private static final String SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR = "prev_address";
  private static final String SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_STREET = "street";
  private static final String SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_CITY = "city";
  private static final String SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_COUNTRY = "country";
  private static final String SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_ZIP = "zip";
  private static final String SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_CURRENTOWNER = "property_current_owner";
  private static final String SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_CURRENTOWNER_OBJ_FNAME = "first_name";
  private static final String SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_CURRENTOWNER_OBJ_MNAME = "middle_name";
  private static final String SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_CURRENTOWNER_OBJ_LNAME = "last_name";
  private static final String SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_CURRENTOWNER_OBJ_PHONE = "phone";
  private static final String SOE_FIELD_CUSTOMER_CHILDREN = "children";
  private static final String SOE_FIELD_CUSTOMER_CHILDREN_OBJ_FNAME = "first_name";
  private static final String SOE_FIELD_CUSTOMER_CHILDREN_OBJ_GENDER = "gender";
  private static final String SOE_FIELD_CUSTOMER_CHILDREN_OBJ_AGE = "age";
  private static final String SOE_FIELD_CUSTOMER_VISITEDPLACES = "visited_places";
  private static final String SOE_FIELD_CUSTOMER_VISITEDPLACES_OBJ_COUNTRY = "country";
  private static final String SOE_FIELD_CUSTOMER_VISITEDPLACES_OBJ_CITIES = "cities";

  public static final String SOE_FIELD_ORDER_LIST = "order_list";

  
  private final Set<String> allFields = new HashSet<String>() {{
      add(SOE_FIELD_CUSTOMER_ID);
      add(SOE_FIELD_CUSTOMER_DOCID);
      add(SOE_FIELD_CUSTOMER_GID);
      add(SOE_FIELD_CUSTOMER_FNAME);
      add(SOE_FIELD_CUSTOMER_LNAME);
      add(SOE_FIELD_CUSTOMER_MNAME);
      add(SOE_FIELD_CUSTOMER_BALLANCE);
      add(SOE_FIELD_CUSTOMER_DOB);
      add(SOE_FIELD_CUSTOMER_EMAIL);
      add(SOE_FIELD_CUSTOMER_ISACTIVE);
      add(SOE_FIELD_CUSTOMER_LINEARSCORE);
      add(SOE_FIELD_CUSTOMER_WEIGHTEDSCORE);
      add(SOE_FIELD_CUSTOMER_PHONECOUNTRY);
      add(SOE_FIELD_CUSTOMER_PHONE);
      add(SOE_FIELD_CUSTOMER_AGEGROUP);
      add(SOE_FIELD_CUSTOMER_AGE);
      add(SOE_FIELD_CUSTOMER_URLPROTOCOL);
      add(SOE_FIELD_CUSTOMER_URLSITE);
      add(SOE_FIELD_CUSTOMER_URLDOMAIN);
      add(SOE_FIELD_CUSTOMER_URL);
      add(SOE_FIELD_CUSTOMER_DEVICES);
      add(SOE_FIELD_CUSTOMER_LINKEDDEVICES);
      add(SOE_FIELD_CUSTOMER_ADDRESS);
      add(SOE_FIELD_CUSTOMER_CHILDREN);
      add(SOE_FIELD_CUSTOMER_VISITEDPLACES);
    }};





  protected abstract void setVal(String key, String value);

  protected abstract String getVal(String key);

  protected abstract int increment(String key, int step);


  public Generator(Properties p) {
    properties = p;

    queryLimitMin = Integer.parseInt(p.getProperty(SoeWorkload.SOE_QUERY_LIMIT_MIN,
        SoeWorkload.SOE_QUERY_LIMIT_MIN_DEFAULT));
    queryLimitMax = Integer.parseInt(p.getProperty(SoeWorkload.SOE_QUERY_LIMIT_MAX,
        SoeWorkload.SOE_QUERY_LIMIT_MAX_DEFAULT));
    if (queryLimitMax < queryLimitMin) {
      int buff = queryLimitMax;
      queryLimitMax = queryLimitMin;
      queryLimitMin = buff;
    }

    queryOffsetMin = Integer.parseInt(p.getProperty(SoeWorkload.SOE_QUERY_OFFSET_MIN,
        SoeWorkload.SOE_QUERY_OFFSET_MIN_DEFAULT));
    queryOffsetMax = Integer.parseInt(p.getProperty(SoeWorkload.SOE_QUERY_OFFSET_MAX,
        SoeWorkload.SOE_QUERY_OFFSET_MAX_DEFAULT));
    if (queryOffsetMax < queryOffsetMin) {
      int buff = queryOffsetMax;
      queryOffsetMax = queryOffsetMin;
      queryOffsetMin = buff;
    }


  }



  public final Set<String> getAllFields() {
    return allFields;
  }

  public void putCustomerDocument(String docKey, String docBody) throws Exception {
    HashMap<String, String> tokens = tokenize(docBody);
    String prefix = SOE_DOCUMENT_PREFIX_CUSTOMER + SOE_SYSTEMFIELD_DELIMITER;
    int storageCount = increment(prefix + SOE_SYSTEMFIELD_STORAGEDOCS_COUNT, 1) - 1;

    setVal(prefix + SOE_METAFIELD_DOCID + SOE_SYSTEMFIELD_DELIMITER + storageCount, docKey);
    setVal(prefix + SOE_METAFIELD_INSERTDOC + SOE_SYSTEMFIELD_DELIMITER + storageCount, docBody);

    for (String key : tokens.keySet()){
      String storageKey = prefix + key + SOE_SYSTEMFIELD_DELIMITER + storageCount;
      String value = tokens.get(key);
      if (value != null) {
        setVal(storageKey, value);
      }  else {
        for (int i = (storageCount-1); i>0; i--) {
          String prevKey = prefix + key + SOE_SYSTEMFIELD_DELIMITER + i;
          String prevVal = getVal(prevKey);
          if (prevVal != null) {
            setVal(storageKey, prevVal);
            break;
          }
        }
      }
    }

    //make sure all values are initialized
    if ((!allValuesInitialized) && (storageCount > 1)) {
      boolean nullDetected = false;
      for (String key : tokens.keySet()) {
        for (int i = 0; i< storageCount; i++) {
          String storageKey = prefix + key + SOE_SYSTEMFIELD_DELIMITER + i;
          String storageValue = getVal(storageKey);
          if (storageValue != null) {
            for (int j = i; j>=0; j--) {
              storageKey = prefix + key + SOE_SYSTEMFIELD_DELIMITER + j;
              setVal(storageKey, storageValue);
            }
            break;
          } else {
            nullDetected = true;
          }
        }
      }
      allValuesInitialized = !nullDetected;
    }
  }

  public Pair<String, String> getInsertDocument() {
    return insertDocument;
  }

  public SoeQueryPredicate getPredicate() {
    return soePredicate;
  }


  public ArrayList<SoeQueryPredicate> getPredicatesSequence() {
    return soePredicatesSequence;
  }


  public void buildInsertDocument() {
    String docBody = getVal(buildStorageKey(SOE_DOCUMENT_PREFIX_CUSTOMER, SOE_METAFIELD_INSERTDOC));
    String keyPrefix = SOE_DOCUMENT_PREFIX_CUSTOMER + SOE_SYSTEMFIELD_DELIMITER;
    int docCounter = increment(keyPrefix + SOE_SYSTEMFIELD_INSERTDOC_COUNTER, 1);
    String docKey = SOE_DOCUMENT_PREFIX_CUSTOMER + SOE_SYSTEMFIELD_DELIMITER + docCounter;
    insertDocument = new Pair<String, String>(docKey, docBody);
  }


  public void buildUpdatePredicate() {
    soePredicate = new SoeQueryPredicate();
    soePredicate.setName(SOE_FIELD_CUSTOMER_BALLANCE);
    soePredicate.setValueA("$" + rand.nextInt(99999) + "." + rand.nextInt(99));
  }


  public void buildPagePredicate() {
    soePredicate = new SoeQueryPredicate();
    soePredicate.setName(SOE_FIELD_CUSTOMER_ADDRESS);
    SoeQueryPredicate innerPredicate = new SoeQueryPredicate();
    innerPredicate.setName(SOE_FIELD_CUSTOMER_ADDRESS_OBJ_ZIP);
    innerPredicate.setValueA(getVal(buildStorageKey(SOE_DOCUMENT_PREFIX_CUSTOMER,
        SOE_FIELD_CUSTOMER_ADDRESS,
        SOE_FIELD_CUSTOMER_ADDRESS_OBJ_ZIP)));
    soePredicate.setNestedPredicateA(innerPredicate);
  }


  public void buildSearchPredicatesSequenceN3() {
    SoeQueryPredicate predicate;

    predicate = new SoeQueryPredicate();
    predicate.setName(SOE_FIELD_CUSTOMER_ADDRESS);
    predicate.setNestedPredicateA(new SoeQueryPredicate());
    predicate.getNestedPredicateA().setName(SOE_FIELD_CUSTOMER_ADDRESS_OBJ_COUNTRY);
    predicate.getNestedPredicateA().setValueA(getVal(buildStorageKey(SOE_DOCUMENT_PREFIX_CUSTOMER,
        SOE_FIELD_CUSTOMER_ADDRESS,
        SOE_FIELD_CUSTOMER_ADDRESS_OBJ_COUNTRY)));
    soePredicatesSequence.add(predicate);


    predicate = new SoeQueryPredicate();
    predicate.setName(SOE_FIELD_CUSTOMER_AGEGROUP);
    predicate.setValueA(getVal(buildStorageKey(SOE_DOCUMENT_PREFIX_CUSTOMER, SOE_FIELD_CUSTOMER_AGEGROUP)));
    soePredicatesSequence.add(predicate);


    predicate = new SoeQueryPredicate();
    predicate.setName(SOE_FIELD_CUSTOMER_DOB);
    predicate.setValueA(getVal(buildStorageKey(SOE_DOCUMENT_PREFIX_CUSTOMER, SOE_FIELD_CUSTOMER_DOB)));
    soePredicatesSequence.add(predicate);

  }


  public void buildNestedScanPredicate() {
    SoeQueryPredicate predicate = new SoeQueryPredicate();
    predicate.setName(SOE_FIELD_CUSTOMER_ADDRESS);
    predicate.setNestedPredicateA(new SoeQueryPredicate());
    predicate.getNestedPredicateA().setName(SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR);
    predicate.getNestedPredicateA().setNestedPredicateA(new SoeQueryPredicate());
    predicate.getNestedPredicateA().getNestedPredicateA().setName(SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_ZIP);
    String key = buildStorageKey(SOE_DOCUMENT_PREFIX_CUSTOMER,
        SOE_FIELD_CUSTOMER_ADDRESS, SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR,
        SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_ZIP);
    predicate.getNestedPredicateA().getNestedPredicateA().setValueA(getVal(key));
    predicate.getNestedPredicateA().getNestedPredicateA().setType(SoeQueryPredicate.SOE_PREDICATE_TYPE_INTEGER);
  }


  public void buildArrayScanPredicate() {
    SoeQueryPredicate predicate = new SoeQueryPredicate();
    predicate.setName(SOE_FIELD_CUSTOMER_DEVICES);
    predicate.setValueA(getVal(buildStorageKey(SOE_DOCUMENT_PREFIX_CUSTOMER, SOE_FIELD_CUSTOMER_DEVICES)));
  }


  public void buildArrayDeepScanPredicate() {
    SoeQueryPredicate predicate = new SoeQueryPredicate();
    predicate.setName(SOE_FIELD_CUSTOMER_VISITEDPLACES);
    predicate.setNestedPredicateA(new SoeQueryPredicate());
    predicate.getNestedPredicateA().setName(SOE_FIELD_CUSTOMER_VISITEDPLACES_OBJ_COUNTRY);
    predicate.getNestedPredicateA().setValueA(getVal(buildStorageKey(SOE_DOCUMENT_PREFIX_CUSTOMER,
        SOE_FIELD_CUSTOMER_VISITEDPLACES, SOE_FIELD_CUSTOMER_VISITEDPLACES_OBJ_COUNTRY)));
    predicate.setNestedPredicateB(new SoeQueryPredicate());
    predicate.getNestedPredicateB().setName(SOE_FIELD_CUSTOMER_VISITEDPLACES_OBJ_COUNTRY);
    predicate.getNestedPredicateB().setValueA(getVal(buildStorageKey(SOE_DOCUMENT_PREFIX_CUSTOMER,
        SOE_FIELD_CUSTOMER_VISITEDPLACES, SOE_FIELD_CUSTOMER_VISITEDPLACES_OBJ_CITIES)));
  }


  public void buildReport1Predicate() {
    // todo
  }


  public void buildReport2Predicate() {
    // todo
  }


  public void buildSyncPredicate() {
    // todo
  }


  public String getRandomCustomerId() {
    if (totalDocsCount == 0) {
      totalDocsCount = Integer.parseInt(getVal(SOE_DOCUMENT_PREFIX_CUSTOMER + SOE_SYSTEMFIELD_DELIMITER +
          SOE_SYSTEMFIELD_TOTALDOCS_COUNT));
    }
    return SOE_DOCUMENT_PREFIX_CUSTOMER + SOE_SYSTEMFIELD_DELIMITER + rand.nextInt(totalDocsCount);
  }

  public int getRandomLimit(){
    if (queryLimitMax == queryLimitMin) {
      return rand.nextInt(queryLimitMin) + 1;
    }
    return rand.nextInt(queryLimitMax - queryLimitMin) + queryLimitMin;
  }

  public int getRandomOffset(){

    if (queryOffsetMax == queryOffsetMin) {
      return rand.nextInt(queryOffsetMin) + 1;
    }
    return rand.nextInt(queryOffsetMax - queryOffsetMin) + queryOffsetMin;
  }
  
  public String getRandomOrderId() {
    if (totalDocsCount == 0) {
      totalDocsCount = Integer.parseInt(getVal(SOE_DOCUMENT_PREFIX_CUSTOMER + SOE_SYSTEMFIELD_DELIMITER +
          SOE_SYSTEMFIELD_TOTALDOCS_COUNT));
    }
    return SOE_DOCUMENT_PREFIX_ORDER + SOE_SYSTEMFIELD_DELIMITER + rand.nextInt(totalDocsCount);
  }


  private String buildStorageKey(String token1) {
    if (storedDocsCount == 0) {
      storedDocsCount = Integer.parseInt(getVal(SOE_DOCUMENT_PREFIX_CUSTOMER + SOE_SYSTEMFIELD_DELIMITER +
          SOE_SYSTEMFIELD_STORAGEDOCS_COUNT));
    }
    return token1 + SOE_SYSTEMFIELD_DELIMITER + rand.nextInt(storedDocsCount);
  }

  private String buildStorageKey(String token1, String token2) {
    return token1 + SOE_SYSTEMFIELD_DELIMITER + buildStorageKey(token2);
  }

  private String buildStorageKey(String token1, String token2, String token3) {
    return token1 + SOE_SYSTEMFIELD_DELIMITER + buildStorageKey(token2, token3);
  }

  private String buildStorageKey(String token1, String token2, String token3, String token4) {
    return token1 + SOE_SYSTEMFIELD_DELIMITER + buildStorageKey(token2, token3, token4);
  }

  private String buildStorageKey(String token1, String token2, String token3, String token4, String token5) {
    return token1 + SOE_SYSTEMFIELD_DELIMITER + buildStorageKey(token2, token3, token4, token5);
  }


  private HashMap<String, String> tokenize(String jsonString) {
    HashMap<String, String> tokens = new HashMap<String, String>();
    JSONObject obj = new JSONObject(jsonString);

    try {
      tokenizeFields(obj, tokens);
    } catch (JSONException ex) {
      System.err.println("Document parsing error - plain fields");
      ex.printStackTrace();
    }

    try {
      tokenizeArrays(obj, tokens);
    } catch (JSONException ex) {
      System.err.println("Document parsing error - arrays");
      ex.printStackTrace();
    }

    try {
      tokenizeObjects(obj, tokens);
    } catch (JSONException ex) {
      System.err.println("Document parsing error - objects");
      ex.printStackTrace();
    }

    return tokens;
  }


  private void tokenizeFields(JSONObject obj, HashMap<String, String> tokens) {



    //string
    ArrayList<String> stringFields = new ArrayList<>(Arrays.asList(SOE_FIELD_CUSTOMER_ID,
        SOE_FIELD_CUSTOMER_GID, SOE_FIELD_CUSTOMER_FNAME, SOE_FIELD_CUSTOMER_LNAME, SOE_FIELD_CUSTOMER_MNAME,
        SOE_FIELD_CUSTOMER_BALLANCE, SOE_FIELD_CUSTOMER_DOB, SOE_FIELD_CUSTOMER_EMAIL, SOE_FIELD_CUSTOMER_PHONECOUNTRY,
        SOE_FIELD_CUSTOMER_PHONE, SOE_FIELD_CUSTOMER_AGEGROUP, SOE_FIELD_CUSTOMER_URLPROTOCOL,
        SOE_FIELD_CUSTOMER_URLSITE, SOE_FIELD_CUSTOMER_URLDOMAIN, SOE_FIELD_CUSTOMER_URL));

    for (String field : stringFields) {
      tokens.put(field, null);
      if (obj.has(field) && !obj.isNull(field)) {
        tokens.put(field, obj.getString(field));
      }
    }

    //integer
    ArrayList<String> intFields = new ArrayList<>(Arrays.asList(SOE_FIELD_CUSTOMER_DOCID,
        SOE_FIELD_CUSTOMER_LINEARSCORE, SOE_FIELD_CUSTOMER_WEIGHTEDSCORE, SOE_FIELD_CUSTOMER_AGE));

    for (String field : intFields) {
      tokens.put(field, null);
      if (obj.has(field) && !obj.isNull(field)) {
        tokens.put(field, String.valueOf(obj.getInt(field)));
      }
    }

    //boolean
    String field = SOE_FIELD_CUSTOMER_ISACTIVE;
    tokens.put(field, null);
    if (obj.has(field) && !obj.isNull(field)) {
      tokens.put(field, String.valueOf(obj.getBoolean(field)));
    }


  }

  private void tokenizeArrays(JSONObject obj, HashMap<String, String>  tokens) {

    //array
    String field = SOE_FIELD_CUSTOMER_DEVICES;
    tokens.put(field, null);
    if (obj.has(field) && !obj.isNull(field)) {
      JSONArray arr = obj.getJSONArray(field);
      if (arr.length() > 0) {
        int element = rand.nextInt(arr.length());
        tokens.put(field, arr.getString(element));
      }
    }

    //array of arrays
    field = SOE_FIELD_CUSTOMER_LINKEDDEVICES;
    tokens.put(field, null);
    if (obj.has(field) && !obj.isNull(field)) {
      JSONArray arr = obj.getJSONArray(field);
      if (arr.length() > 0) {
        int element = rand.nextInt(arr.length());
        JSONArray inarr = arr.getJSONArray(element);
        if (inarr.length() > 0) {
          int inelement = rand.nextInt(inarr.length());
          tokens.put(field, inarr.getString(inelement));
        }
      }
    }

    //array of objects
    field = SOE_FIELD_CUSTOMER_CHILDREN;
    tokens.put(field + SOE_SYSTEMFIELD_DELIMITER + SOE_FIELD_CUSTOMER_CHILDREN_OBJ_GENDER, null);
    tokens.put(field + SOE_SYSTEMFIELD_DELIMITER + SOE_FIELD_CUSTOMER_CHILDREN_OBJ_FNAME, null);
    tokens.put(field + SOE_SYSTEMFIELD_DELIMITER + SOE_FIELD_CUSTOMER_CHILDREN_OBJ_AGE, null);

    if (obj.has(field) && !obj.isNull(field)) {
      JSONArray inarr = obj.getJSONArray(field);
      if (inarr.length() > 0) {
        int element = rand.nextInt(inarr.length());
        JSONObject inobj = inarr.getJSONObject(element);
        ArrayList<String> inobjStringFields = new ArrayList<>(Arrays.asList(
            SOE_FIELD_CUSTOMER_CHILDREN_OBJ_GENDER, SOE_FIELD_CUSTOMER_CHILDREN_OBJ_FNAME));

        for (String infield : inobjStringFields) {
          if (inobj.has(infield) && !inobj.isNull(infield)) {
            String key = field + SOE_SYSTEMFIELD_DELIMITER + infield;
            tokens.put(key, inobj.getString(infield));
          }
        }
        String infield = SOE_FIELD_CUSTOMER_CHILDREN_OBJ_AGE;
        if (inobj.has(infield) && !inobj.isNull(infield)) {
          String key = field + SOE_SYSTEMFIELD_DELIMITER + infield;
          tokens.put(key, String.valueOf(inobj.getInt(infield)));
        }
      }
    }

    //array of objects with array
    field = SOE_FIELD_CUSTOMER_VISITEDPLACES;
    tokens.put(field + SOE_SYSTEMFIELD_DELIMITER + SOE_FIELD_CUSTOMER_VISITEDPLACES_OBJ_COUNTRY, null);
    tokens.put(field + SOE_SYSTEMFIELD_DELIMITER + SOE_FIELD_CUSTOMER_VISITEDPLACES_OBJ_CITIES, null);

    if (obj.has(field) && !obj.isNull(field)) {
      JSONArray inarr = obj.getJSONArray(field);
      if (inarr.length()>0) {
        int element = rand.nextInt(inarr.length());
        JSONObject inobj = inarr.getJSONObject(element);
        String infield = SOE_FIELD_CUSTOMER_VISITEDPLACES_OBJ_COUNTRY;
        if (inobj.has(infield) && !inobj.isNull(infield)) {
          String key = field + SOE_SYSTEMFIELD_DELIMITER + infield;
          tokens.put(key, inobj.getString(infield));
        }
        infield = SOE_FIELD_CUSTOMER_VISITEDPLACES_OBJ_CITIES;
        if (inobj.has(infield) && !inobj.isNull(infield)) {
          JSONArray inarr2 = inobj.getJSONArray(infield);
          if (inarr2.length() >0) {
            int inelement2 = rand.nextInt(inarr2.length());
            String key = field + SOE_SYSTEMFIELD_DELIMITER + infield;
            tokens.put(key, inarr2.getString(inelement2));
          }
        }
      }
    }
  }

  private void tokenizeObjects(JSONObject obj, HashMap<String, String>  tokens) {

    //3-level nested objects
    String field = SOE_FIELD_CUSTOMER_ADDRESS;

    String l1Prefix = field + SOE_SYSTEMFIELD_DELIMITER;
    tokens.put(l1Prefix + SOE_FIELD_CUSTOMER_ADDRESS_OBJ_CITY, null);
    tokens.put(l1Prefix + SOE_FIELD_CUSTOMER_ADDRESS_OBJ_ZIP, null);
    tokens.put(l1Prefix + SOE_FIELD_CUSTOMER_ADDRESS_OBJ_COUNTRY, null);
    tokens.put(l1Prefix + SOE_FIELD_CUSTOMER_ADDRESS_OBJ_STREET, null);

    String l2Prefix = l1Prefix + SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR + SOE_SYSTEMFIELD_DELIMITER;
    tokens.put(l2Prefix + SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_CITY, null);
    tokens.put(l2Prefix + SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_ZIP, null);
    tokens.put(l2Prefix + SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_STREET, null);
    tokens.put(l2Prefix + SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_COUNTRY, null);

    String l3Prefix = l2Prefix + SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_CURRENTOWNER + SOE_SYSTEMFIELD_DELIMITER;
    tokens.put(l3Prefix + SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_CURRENTOWNER_OBJ_FNAME, null);
    tokens.put(l3Prefix + SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_CURRENTOWNER_OBJ_LNAME, null);
    tokens.put(l3Prefix + SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_CURRENTOWNER_OBJ_MNAME, null);
    tokens.put(l3Prefix + SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_CURRENTOWNER_OBJ_PHONE, null);


    if (obj.has(field) && !obj.isNull(field)) {
      JSONObject inobj = obj.getJSONObject(field);

      ArrayList<String> inobjStringFields = new ArrayList<>(Arrays.asList(
          SOE_FIELD_CUSTOMER_ADDRESS_OBJ_CITY,
          SOE_FIELD_CUSTOMER_ADDRESS_OBJ_ZIP,
          SOE_FIELD_CUSTOMER_ADDRESS_OBJ_COUNTRY,
          SOE_FIELD_CUSTOMER_ADDRESS_OBJ_STREET));

      for (String infield : inobjStringFields) {
        if (inobj.has(infield) && !inobj.isNull(infield)) {
          String key = field + SOE_SYSTEMFIELD_DELIMITER + infield;
          tokens.put(key, inobj.getString(infield));
        }
      }

      String infield = SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR;
      if (inobj.has(infield) && !inobj.isNull(infield)) {
        JSONObject inobj2 = inobj.getJSONObject(infield);

        ArrayList<String> inobj2StringFields = new ArrayList<>(Arrays.asList(
            SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_CITY,
            SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_ZIP,
            SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_STREET,
            SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_COUNTRY));

        for (String infield2 : inobj2StringFields) {
          if (inobj2.has(infield2) && !inobj2.isNull(infield2)) {
            String key = field + SOE_SYSTEMFIELD_DELIMITER + infield + SOE_SYSTEMFIELD_DELIMITER + infield2;
            tokens.put(key, inobj2.getString(infield2));
          }
        }

        String infield2 = SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_CURRENTOWNER;
        if (inobj2.has(infield2) && !inobj2.isNull(infield2)) {
          JSONObject inobj3 = inobj2.getJSONObject(infield2);
          ArrayList<String> inobj3StringFields = new ArrayList<>(Arrays.asList(
              SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_CURRENTOWNER_OBJ_FNAME,
              SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_CURRENTOWNER_OBJ_LNAME,
              SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_CURRENTOWNER_OBJ_MNAME,
              SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_CURRENTOWNER_OBJ_PHONE));

          for (String infield3 : inobj3StringFields) {
            if (inobj3.has(infield3) && !inobj3.isNull(infield3)) {
              String key = field + SOE_SYSTEMFIELD_DELIMITER +
                  infield + SOE_SYSTEMFIELD_DELIMITER + infield2 + SOE_SYSTEMFIELD_DELIMITER + infield3;
              tokens.put(key, inobj3.getString(infield3));
            }
          }
        }
      }
    }
  }
}
