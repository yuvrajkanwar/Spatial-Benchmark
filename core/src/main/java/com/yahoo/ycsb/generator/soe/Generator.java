package com.yahoo.ycsb.generator.soe;

import com.yahoo.ycsb.generator.ZipfianGenerator;
import com.yahoo.ycsb.workloads.soe.SoeQueryPredicate;
import com.yahoo.ycsb.workloads.soe.GeoQueryPredicate;
import com.yahoo.ycsb.workloads.soe.SoeWorkload;

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
  private int storedDocsCountCustomer = 0;
  private int storedDocsCountIncidents = 0;
  private int storedDocsCountOrder = 0;
  private Random rand = new Random();

  private boolean allValuesInitialized = false;
  private Properties properties;
  private int queryLimitMin = 0;
  private int queryLimitMax = 0;
  private int queryOffsetMin = 0;
  private int queryOffsetMax = 0;

  private boolean isZipfian = false;
  private ZipfianGenerator zipfianGenerator = null;

  private SoeQueryPredicate soePredicate;
  private GeoQueryPredicate geoPredicate;
  private ArrayList<SoeQueryPredicate> soePredicatesSequence;

  public static final String GEO_DOCUMENT_PREFIX_INCIDENTS = "incidents";
  public static final String SOE_DOCUMENT_PREFIX_CUSTOMER = "customer";
  public static final String SOE_DOCUMENT_PREFIX_ORDER = "order";

  public static final String SOE_SYSTEMFIELD_DELIMITER = ":::";
  public static final String GEO_SYSTEMFIELD_DELIMITER = ":::";
  public static final String SOE_SYSTEMFIELD_INSERTDOC_COUNTER = "SOE_insert_document_counter";
  public static final String GEO_SYSTEMFIELD_INSERTDOC_COUNTER = "GEO_insert_document_counter";
  public static final String SOE_SYSTEMFIELD_STORAGEDOCS_COUNT_CUSTOMER = "SOE_storage_docs_count_customer";
  public static final String GEO_SYSTEMFIELD_STORAGEDOCS_COUNT_INCIDENTS = "GEO_storage_docs_count_incidents";
  public static final String SOE_SYSTEMFIELD_STORAGEDOCS_COUNT_ORDER = "SOE_storage_docs_count_order";
  public static final String SOE_SYSTEMFIELD_TOTALDOCS_COUNT = "SOE_total_docs_count";
  public static final String GEO_SYSTEMFIELD_TOTALDOCS_COUNT = "GEO_total_docs_count";

  private static final String SOE_METAFIELD_DOCID = "SOE_doc_id";
  private static final String GEO_METAFIELD_DOCID = "GEO_doc_id";
  private static final String SOE_METAFIELD_INSERTDOC = "SOE_insert_document";
  private static final String GEO_METAFIELD_INSERTDOC = "GEO_insert_document";

  private static final String GEO_FIELD_INCIDENTS_ID = "_id";
  private static final String GEO_FIELD_INCIDENTS_TYPE = "type";
  private static final String GEO_FIELD_INCIDENTS_PROPERTIES = "properties";
  private static final String GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_OBJECTID = "OBJECTID";
  private static final String GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_INCIDENT_NUMBER = "INCIDENT_NUMBER";
  private static final String GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_LOCATION = "LOCATION";
  private static final String GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_NOTIFICATION = "NOTIFICATION";
  private static final String GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_INCIDENT_DATE = "INCIDENT_DATE";
  private static final String GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_TAG_COUNT = "TAG_COUNT";
  private static final String GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_MONIKER_CLASS = "MONIKER_CLASS";
  private static final String GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_SQ_FT = "SQ_FT";
  private static final String GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_PROP_TYPE = "PROP_TYPE";
  private static final String GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_WAIVER = "Waiver";
  private static final String GEO_FIELD_INCIDENTS_GEOMETRY = "geometry";
  private static final String GEO_FIELD_INCIDENTS_GEOMETRY_OBJ_TYPE = "type";
  private static final String GEO_FIELD_INCIDENTS_GEOMETRY_OBJ_COORDINATES = "coordinates";

  private static final String SOE_FIELD_CUSTOMER_ID = "_id";
  private static final String SOE_FIELD_CUSTOMER_DOCID = "doc_id";
  private static final String SOE_FIELD_CUSTOMER_GID = "gid";
  private static final String SOE_FIELD_CUSTOMER_FNAME = "first_name";
  private static final String SOE_FIELD_CUSTOMER_LNAME = "middle_name";
  private static final String SOE_FIELD_CUSTOMER_MNAME = "last_name";
  private static final String SOE_FIELD_CUSTOMER_BALLANCE = "ballance_current";
  public static final String SOE_FIELD_CUSTOMER_DOB = "dob";
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
  private static final String SOE_FIELD_CUSTOMER_ADDRESS_OBJ_GEOREGION = "geo_region";
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
  public static final String SOE_FIELD_CUSTOMER_ORDER_LIST = "order_list";

  private static final String SOE_FIELD_ORDER_ID = "_id";
  private static final String SOE_FIELD_ORDER_CUSTOMERID = "customer_id";
  private static final String SOE_FIELD_ORDER_YEAR = "year";
  private static final String SOE_FIELD_ORDER_MONTH = "month";
  private static final String SOE_FIELD_ORDER_DAY = "day";
  private static final String SOE_FIELD_ORDER_WEEKDAY = "weekday";
  private static final String SOE_FIELD_ORDER_QUANTITY = "quantity";
  private static final String SOE_FIELD_ORDER_LISTPRICE = "list_price";
  private static final String SOE_FIELD_ORDER_DISCOUNT = "discount_amount_percent";
  private static final String SOE_FIELD_ORDER_SALEPRICE = "sale_price";
  private static final String SOE_FIELD_ORDER_TAX = "tax";
  private static final String SOE_FIELD_ORDER_COUPON = "coupon";
  private static final String SOE_FIELD_ORDER_DEPARTMNET = "department";
  private static final String SOE_FIELD_ORDER_PRODUCTNAME = "product_name";


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
      add(SOE_FIELD_CUSTOMER_ORDER_LIST);
    }};

  private final Set<String> allGeoFields = new HashSet<String>() {{
      add(GEO_FIELD_INCIDENTS_ID);
      add(GEO_FIELD_INCIDENTS_TYPE);
      add(GEO_FIELD_INCIDENTS_PROPERTIES);
      add(GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_OBJECTID);
      add(GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_INCIDENT_NUMBER);
      add(GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_LOCATION);
      add(GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_NOTIFICATION);
      add(GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_INCIDENT_DATE);
      add(GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_TAG_COUNT);
      add(GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_MONIKER_CLASS);
      add(GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_SQ_FT);
      add(GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_PROP_TYPE);
      add(GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_WAIVER);
      add(GEO_FIELD_INCIDENTS_GEOMETRY);
      add(GEO_FIELD_INCIDENTS_GEOMETRY_OBJ_TYPE);
      add(GEO_FIELD_INCIDENTS_GEOMETRY_OBJ_COORDINATES);
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

    isZipfian = p.getProperty(SoeWorkload.SOE_REQUEST_DISTRIBUTION,
        SoeWorkload.SOE_REQUEST_DISTRIBUTION_DEFAULT).equals("zipfian");
  }

  public final Set<String> getAllFields() {
    return allFields;
  }
  public final Set<String> getAllGeoFields() {
    return allGeoFields;
  }

  public void putIncidentsDocument(String docKey, String docBody) throws Exception {
    HashMap<String, String> tokens = tokenize(docBody);
    String prefix = GEO_DOCUMENT_PREFIX_INCIDENTS + GEO_SYSTEMFIELD_DELIMITER;
    int storageCount = increment(prefix + GEO_SYSTEMFIELD_STORAGEDOCS_COUNT_INCIDENTS, 1) - 1;

    setVal(prefix + GEO_METAFIELD_DOCID + GEO_SYSTEMFIELD_DELIMITER + storageCount, docKey);
    setVal(prefix + GEO_METAFIELD_INSERTDOC + GEO_SYSTEMFIELD_DELIMITER + storageCount, docBody);

    for (String key : tokens.keySet()){
      String storageKey = prefix + key + GEO_SYSTEMFIELD_DELIMITER + storageCount;
      String value = tokens.get(key);
      if (value != null) {
        setVal(storageKey, value);
      }  else {
        for (int i = (storageCount-1); i>0; i--) {
          String prevKey = prefix + key + GEO_SYSTEMFIELD_DELIMITER + i;
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
          String storageKey = prefix + key + GEO_SYSTEMFIELD_DELIMITER + i;
          String storageValue = getVal(storageKey);
          if (storageValue != null) {
            for (int j = i; j>=0; j--) {
              storageKey = prefix + key + GEO_SYSTEMFIELD_DELIMITER + j;
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

  public void putCustomerDocument(String docKey, String docBody) throws Exception {
    HashMap<String, String> tokens = tokenize(docBody);
    String prefix = SOE_DOCUMENT_PREFIX_CUSTOMER + SOE_SYSTEMFIELD_DELIMITER;
    int storageCount = increment(prefix + SOE_SYSTEMFIELD_STORAGEDOCS_COUNT_CUSTOMER, 1) - 1;

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

  public void putOrderDocument(String docKey, String docBody) throws Exception {
    HashMap<String, String> tokens = tokenizeOrderFields(docBody);

    String prefix = SOE_DOCUMENT_PREFIX_ORDER + SOE_SYSTEMFIELD_DELIMITER;
    int storageCount = increment(prefix + SOE_SYSTEMFIELD_STORAGEDOCS_COUNT_ORDER, 1) - 1;

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
  }


  public SoeQueryPredicate getPredicate() {
    return soePredicate;
  }

  public GeoQueryPredicate getGeoPredicate() {
    return geoPredicate;
  }

  public ArrayList<SoeQueryPredicate> getPredicatesSequence() {
    return soePredicatesSequence;
  }

  public void buildInsertDocument() {
    //String storageKey = buildStorageKey(SOE_DOCUMENT_PREFIX_CUSTOMER, SOE_METAFIELD_INSERTDOC);
    String storageKey = SOE_DOCUMENT_PREFIX_CUSTOMER + SOE_SYSTEMFIELD_DELIMITER +
        SOE_METAFIELD_INSERTDOC + SOE_SYSTEMFIELD_DELIMITER + getNumberRandom(getStoredCustomersCount());

    String docBody = getVal(storageKey);
    String keyPrefix = SOE_DOCUMENT_PREFIX_CUSTOMER + SOE_SYSTEMFIELD_DELIMITER;
    int docCounter = increment(keyPrefix + SOE_SYSTEMFIELD_INSERTDOC_COUNTER, 1);

    soePredicate = new SoeQueryPredicate();
    soePredicate.setDocid(keyPrefix + docCounter);
    soePredicate.setValueA(docBody);

  }

  public void buildGeoReadPredicate() {
    buildGeoInsertDocument();
    GeoQueryPredicate queryPredicate = new GeoQueryPredicate();
    queryPredicate.setName(GEO_FIELD_INCIDENTS_GEOMETRY);
    double[] latLong = {-111-rand.nextDouble(), 33+rand.nextDouble()};
    JSONArray jsonArray = new JSONArray(latLong);
    JSONObject jobj = new JSONObject().put("type", "Point");
    jobj.put("coordinates", jsonArray);
    queryPredicate.setValueA(jobj);


    buildGeoInsertDocument();
    GeoQueryPredicate queryPredicate2 = new GeoQueryPredicate();
    queryPredicate2.setName(GEO_FIELD_INCIDENTS_GEOMETRY);
    double[] latLong2 = {-111-rand.nextDouble(), 33+rand.nextDouble()};
    JSONArray jsonArray2 = new JSONArray(latLong2);
    JSONObject jobj2 = new JSONObject().put("type", "Point");
    jobj2.put("coordinates", jsonArray2);
    queryPredicate2.setValueA(jobj2);

    buildGeoInsertDocument();
    GeoQueryPredicate queryPredicate3 = new GeoQueryPredicate();
    queryPredicate3.setName(GEO_FIELD_INCIDENTS_GEOMETRY);
    double[] latLong3 = {-111-rand.nextDouble(), 33+rand.nextDouble()};
    double[] latLong4 = {-111-rand.nextDouble(), 33+rand.nextDouble()};
    JSONArray jsonArray3 = new JSONArray(latLong3);
    JSONArray jsonArray4 = new JSONArray(latLong4);
    JSONArray jsonArray5 = new JSONArray();
    JSONArray jsonArray6 = new JSONArray();
    JSONArray jsonArray7 = new JSONArray();
    jsonArray5.put(jsonArray);
    jsonArray5.put(jsonArray2);
    jsonArray6.put(jsonArray3);
    jsonArray6.put(jsonArray4);
    jsonArray7.put(jsonArray5);
    jsonArray7.put(jsonArray6);
    JSONObject jobj3 = new JSONObject().put("type", "MultiLineString");
    jobj3.put("coordinates", jsonArray7);
    queryPredicate3.setValueA(jobj3);
    geoPredicate.setNestedPredicateC(queryPredicate3);
    geoPredicate.setNestedPredicateB(queryPredicate2);
    geoPredicate.setNestedPredicateA(queryPredicate);


  }

  public void buildGeoInsertDocument() {
    //String storageKey = buildStorageKey(SOE_DOCUMENT_PREFIX_CUSTOMER, SOE_METAFIELD_INSERTDOC);
    String storageKey = GEO_DOCUMENT_PREFIX_INCIDENTS + GEO_SYSTEMFIELD_DELIMITER +
        GEO_METAFIELD_INSERTDOC + GEO_SYSTEMFIELD_DELIMITER + getNumberRandom(getStoredIncidentsCount());

    String docBody = getVal(storageKey);
    String keyPrefix = GEO_DOCUMENT_PREFIX_INCIDENTS + GEO_SYSTEMFIELD_DELIMITER;
    int docCounter = increment(keyPrefix + GEO_SYSTEMFIELD_INSERTDOC_COUNTER, 1);

    geoPredicate = new GeoQueryPredicate();
    geoPredicate.setDocid(keyPrefix + docCounter);
    geoPredicate.setValue(docBody);

  }

  // building value as random to make sure the original value is overwritten with new one
  public void buildUpdatePredicate() {
    buildInsertDocument();
    SoeQueryPredicate queryPredicate = new SoeQueryPredicate();
    queryPredicate.setName(SOE_FIELD_CUSTOMER_BALLANCE);
    queryPredicate.setValueA("$" + rand.nextInt(99999) + "." + rand.nextInt(99));
    soePredicate.setNestedPredicateA(queryPredicate);
  }

  public void buildGeoUpdatePredicate() {
    buildGeoInsertDocument();
    GeoQueryPredicate queryPredicate = new GeoQueryPredicate();
    queryPredicate.setName(GEO_FIELD_INCIDENTS_GEOMETRY);
    double[] latLong = {-111-rand.nextDouble(), 33+rand.nextDouble()};
    JSONArray jsonArray = new JSONArray(latLong);
    JSONObject jobj = new JSONObject().put("type", "Point");
    jobj.put("coordinates", jsonArray);
    queryPredicate.setValueA(jobj);
    geoPredicate.setNestedPredicateA(queryPredicate);
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

  public void buildPage2Predicate() {
    soePredicate = new SoeQueryPredicate();
    soePredicate.setName(SOE_FIELD_CUSTOMER_ADDRESS);
    SoeQueryPredicate innerPredicate = new SoeQueryPredicate();
    innerPredicate.setName(SOE_FIELD_CUSTOMER_ADDRESS_OBJ_GEOREGION);
    innerPredicate.setValueA(getVal(buildStorageKey(SOE_DOCUMENT_PREFIX_CUSTOMER,
        SOE_FIELD_CUSTOMER_ADDRESS,
        SOE_FIELD_CUSTOMER_ADDRESS_OBJ_GEOREGION)));
    soePredicate.setNestedPredicateA(innerPredicate);
  }


  public void buildSearchPredicatesSequenceN3() {
    SoeQueryPredicate predicate;
    soePredicatesSequence = new ArrayList<SoeQueryPredicate>();

    predicate = new SoeQueryPredicate();
    predicate.setName(SOE_FIELD_CUSTOMER_ADDRESS);
    SoeQueryPredicate innerPredicate = new SoeQueryPredicate();
    innerPredicate.setName(SOE_FIELD_CUSTOMER_ADDRESS_OBJ_COUNTRY);
    innerPredicate.setValueA(getVal(buildStorageKey(SOE_DOCUMENT_PREFIX_CUSTOMER,
        SOE_FIELD_CUSTOMER_ADDRESS,
        SOE_FIELD_CUSTOMER_ADDRESS_OBJ_COUNTRY)));
    predicate.setNestedPredicateA(innerPredicate);
    soePredicatesSequence.add(predicate);

    predicate = new SoeQueryPredicate();
    predicate.setName(SOE_FIELD_CUSTOMER_AGEGROUP);
    predicate.setValueA(getVal(buildStorageKey(SOE_DOCUMENT_PREFIX_CUSTOMER, SOE_FIELD_CUSTOMER_AGEGROUP)));
    soePredicatesSequence.add(predicate);

    predicate = new SoeQueryPredicate();
    predicate.setName(SOE_FIELD_CUSTOMER_DOB);
    String dob = getVal(buildStorageKey(SOE_DOCUMENT_PREFIX_CUSTOMER,
        SOE_FIELD_CUSTOMER_DOB));
    try {
      predicate.setValueA(dob.split("-")[0]);
    } catch (Exception e) {
      System.err.println("failed to get year out of DOB" + e.getMessage());
      predicate.setValueA("2017");
    }
    soePredicatesSequence.add(predicate);
  }


  public void buildSearch2PredicatesSequenceN3() {
    SoeQueryPredicate predicate;
    soePredicatesSequence = new ArrayList<SoeQueryPredicate>();

    predicate = new SoeQueryPredicate();
    predicate.setName(SOE_FIELD_CUSTOMER_ADDRESS);
    SoeQueryPredicate innerPredicate = new SoeQueryPredicate();
    innerPredicate.setName(SOE_FIELD_CUSTOMER_ADDRESS_OBJ_GEOREGION);
    innerPredicate.setValueA(getVal(buildStorageKey(SOE_DOCUMENT_PREFIX_CUSTOMER,
        SOE_FIELD_CUSTOMER_ADDRESS,
        SOE_FIELD_CUSTOMER_ADDRESS_OBJ_GEOREGION)));
    predicate.setNestedPredicateA(innerPredicate);
    soePredicatesSequence.add(predicate);

    predicate = new SoeQueryPredicate();
    predicate.setName(SOE_FIELD_CUSTOMER_AGEGROUP);
    predicate.setValueA(getVal(buildStorageKey(SOE_DOCUMENT_PREFIX_CUSTOMER, SOE_FIELD_CUSTOMER_AGEGROUP)));
    soePredicatesSequence.add(predicate);

    predicate = new SoeQueryPredicate();
    predicate.setName(SOE_FIELD_CUSTOMER_DOB);
    String dob = getVal(buildStorageKey(SOE_DOCUMENT_PREFIX_CUSTOMER,
        SOE_FIELD_CUSTOMER_DOB));
    try {
      predicate.setValueA(dob.split("-")[0]);
    } catch (Exception e) {
      System.err.println("failed to get year out of DOB" + e.getMessage());
      predicate.setValueA("2017");
    }
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
    soePredicate = predicate;
  }

  public void buildArrayScanPredicate() {
    SoeQueryPredicate predicate = new SoeQueryPredicate();
    predicate.setName(SOE_FIELD_CUSTOMER_DEVICES);
    predicate.setValueA(getVal(buildStorageKey(SOE_DOCUMENT_PREFIX_CUSTOMER, SOE_FIELD_CUSTOMER_DEVICES)));
    soePredicate = predicate;
  }

  public void buildArrayDeepScanPredicate() {
    SoeQueryPredicate predicate = new SoeQueryPredicate();
    predicate.setName(SOE_FIELD_CUSTOMER_VISITEDPLACES);

    int storageKeyOffset = (isZipfian)? getNumberZipfianUnifrom() : getNumberRandom(getStoredCustomersCount());

    String storageKeyPrefix = SOE_DOCUMENT_PREFIX_CUSTOMER + SOE_SYSTEMFIELD_DELIMITER +
        SOE_FIELD_CUSTOMER_VISITEDPLACES + SOE_SYSTEMFIELD_DELIMITER;

    SoeQueryPredicate innerPredicateA = new SoeQueryPredicate();
    innerPredicateA.setName(SOE_FIELD_CUSTOMER_VISITEDPLACES_OBJ_COUNTRY);
    innerPredicateA.setValueA(getVal(storageKeyPrefix +
        SOE_FIELD_CUSTOMER_VISITEDPLACES_OBJ_COUNTRY + SOE_SYSTEMFIELD_DELIMITER + storageKeyOffset));

    SoeQueryPredicate innerPredicateB = new SoeQueryPredicate();
    innerPredicateB.setName(SOE_FIELD_CUSTOMER_VISITEDPLACES_OBJ_CITIES);
    innerPredicateB.setValueA(getVal(storageKeyPrefix +
        SOE_FIELD_CUSTOMER_VISITEDPLACES_OBJ_CITIES + SOE_SYSTEMFIELD_DELIMITER + storageKeyOffset));

    predicate.setNestedPredicateA(innerPredicateA);
    predicate.setNestedPredicateB(innerPredicateB);
    soePredicate = predicate;

  }

  public void buildReport1PredicateSequence() {

    soePredicatesSequence = new ArrayList<>();
    SoeQueryPredicate predicate = new SoeQueryPredicate();
    predicate.setName(SOE_FIELD_CUSTOMER_ORDER_LIST);
    soePredicatesSequence.add(predicate);

    predicate = new SoeQueryPredicate();
    predicate.setName(SOE_FIELD_CUSTOMER_ADDRESS);
    SoeQueryPredicate innerPredicate = new SoeQueryPredicate();
    innerPredicate.setName(SOE_FIELD_CUSTOMER_ADDRESS_OBJ_ZIP);
    innerPredicate.setValueA(getVal(buildStorageKey(SOE_DOCUMENT_PREFIX_CUSTOMER, SOE_FIELD_CUSTOMER_ADDRESS,
        SOE_FIELD_CUSTOMER_ADDRESS_OBJ_ZIP)));
    predicate.setNestedPredicateA(innerPredicate);
    soePredicatesSequence.add(predicate);
  }

  public void buildReport2PredicateSequence() {

    soePredicatesSequence = new ArrayList<>();

    String orderPrefix = SOE_DOCUMENT_PREFIX_ORDER + SOE_SYSTEMFIELD_DELIMITER;

    SoeQueryPredicate oDatePredicate = new SoeQueryPredicate();
    oDatePredicate.setName(SOE_FIELD_ORDER_MONTH);
    oDatePredicate.setValueA(getVal(orderPrefix + SOE_FIELD_ORDER_MONTH + SOE_SYSTEMFIELD_DELIMITER +
        getNumberRandom(getStoredOrdersCount())));
    soePredicatesSequence.add(oDatePredicate);

    SoeQueryPredicate oSalepricePredicate = new SoeQueryPredicate();
    oSalepricePredicate.setName(SOE_FIELD_ORDER_SALEPRICE);
    oSalepricePredicate.setValueA(getVal(orderPrefix + SOE_FIELD_ORDER_SALEPRICE + SOE_SYSTEMFIELD_DELIMITER +
        getNumberRandom(getStoredOrdersCount())));
    soePredicatesSequence.add(oSalepricePredicate);

    SoeQueryPredicate cAddressPredicate = new SoeQueryPredicate();
    cAddressPredicate.setName(SOE_FIELD_CUSTOMER_ADDRESS);
    SoeQueryPredicate cAddressZipPredicate = new SoeQueryPredicate();
    cAddressZipPredicate.setName(SOE_FIELD_CUSTOMER_ADDRESS_OBJ_ZIP);

    cAddressZipPredicate.setValueA(getVal(buildStorageKey(SOE_DOCUMENT_PREFIX_CUSTOMER, SOE_FIELD_CUSTOMER_ADDRESS,
        SOE_FIELD_CUSTOMER_ADDRESS_OBJ_ZIP)));
    cAddressPredicate.setNestedPredicateA(cAddressZipPredicate);
    soePredicatesSequence.add(cAddressPredicate);

    SoeQueryPredicate cOrderList = new SoeQueryPredicate();
    cOrderList.setName(SOE_FIELD_CUSTOMER_ORDER_LIST);
    soePredicatesSequence.add(cOrderList);
  }

  public void buildSyncPredicate() {
    // todo
  }



  public String getCustomerIdRandom() {
    return SOE_DOCUMENT_PREFIX_CUSTOMER + SOE_SYSTEMFIELD_DELIMITER + getNumberRandom(getTotalcustomersCount());
  }

  public String getIncidentsIdRandom() {
    return "" + getNumberRandom(getTotalIncidentsCount());
  }

  public String getCustomerIdWithDistribution() {
    if (isZipfian) {
      return SOE_DOCUMENT_PREFIX_CUSTOMER + SOE_SYSTEMFIELD_DELIMITER +
          getNumberZipfianLatests(getTotalcustomersCount());
    }
    return getCustomerIdRandom();
  }

  public String getIncidentIdWithDistribution() {
    if (isZipfian) {
      return getNumberZipfianLatests(getTotalIncidentsCount())+"";
    }
    return getIncidentsIdRandom();
  }

  public int getRandomLimit(){
    if (queryLimitMax == queryLimitMin) {
      return queryLimitMax;
    }
    return rand.nextInt(queryLimitMax - queryLimitMin + 1) + queryLimitMin;
  }

  public int getRandomOffset(){

    if (queryOffsetMax == queryOffsetMin) {
      return queryOffsetMax;
    }
    return rand.nextInt(queryOffsetMax - queryOffsetMin + 1) + queryOffsetMin;
  }

  private String buildStorageKey(String token1) {
    if (isZipfian) {
      return token1 + SOE_SYSTEMFIELD_DELIMITER + getNumberZipfianUnifrom();
    }
    return token1 + SOE_SYSTEMFIELD_DELIMITER + getNumberRandom(getStoredCustomersCount());
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
      tokenizeObjects(obj, tokens);
    } catch (JSONException ex) {
      System.err.println("Document parsing error - objects");
      ex.printStackTrace();
    }

    return tokens;
  }

  private HashMap<String, String>  tokenizeOrderFields(String jsonString) {
    HashMap<String, String> tokens = new HashMap<String, String>();
    JSONObject obj = new JSONObject(jsonString);

    //string
    ArrayList<String> stringFields = new ArrayList<>(Arrays.asList(SOE_FIELD_ORDER_ID, SOE_FIELD_ORDER_YEAR,
        SOE_FIELD_ORDER_MONTH, SOE_FIELD_ORDER_WEEKDAY, SOE_FIELD_ORDER_COUPON, SOE_FIELD_ORDER_DEPARTMNET,
        SOE_FIELD_ORDER_PRODUCTNAME, SOE_FIELD_ORDER_CUSTOMERID));

    for (String field : stringFields) {
      tokens.put(field, null);
      if (obj.has(field) && !obj.isNull(field)) {
        tokens.put(field, obj.getString(field));
      }
    }

    //integer
    ArrayList<String> intFields = new ArrayList<>(Arrays.asList(SOE_FIELD_ORDER_QUANTITY, SOE_FIELD_ORDER_DISCOUNT,
        SOE_FIELD_ORDER_DAY));

    for (String field : intFields) {
      tokens.put(field, null);
      if (obj.has(field) && !obj.isNull(field)) {
        tokens.put(field, String.valueOf(obj.getInt(field)));
      }
    }

    //float
    ArrayList<String> floatFields = new ArrayList<>(Arrays.asList(SOE_FIELD_ORDER_LISTPRICE,
        SOE_FIELD_ORDER_SALEPRICE, SOE_FIELD_ORDER_TAX));
    for (String field : floatFields) {
      tokens.put(field, null);
      if (obj.has(field) && !obj.isNull(field)) {
        tokens.put(field, String.valueOf(obj.getDouble(field)));
      }
    }

    return tokens;
  }

  private void tokenizeFields(JSONObject obj, HashMap<String, String> tokens) {


    //string
    ArrayList<String> stringFields = new ArrayList<>(Arrays.asList(GEO_FIELD_INCIDENTS_TYPE));

    for (String field : stringFields) {
      tokens.put(field, null);
      if (obj.has(field) && !obj.isNull(field)) {
        tokens.put(field, obj.getString(field));
      }
    }
  }


  private void tokenizeObjects(JSONObject obj, HashMap<String, String>  tokens) {

    String id = GEO_FIELD_INCIDENTS_ID;

    tokens.put(GEO_FIELD_INCIDENTS_ID, null);

    if(obj.has(id) && !obj.isNull(id)){
      JSONObject idobj = obj.getJSONObject(id);
      String key = id;
      tokens.put(key, JSONObject.valueToString(idobj));
    }
    //1-level nested objects
    String field = GEO_FIELD_INCIDENTS_PROPERTIES;

    String l1Prefix = field + GEO_SYSTEMFIELD_DELIMITER;
    tokens.put(l1Prefix + GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_OBJECTID, null);
    tokens.put(l1Prefix + GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_INCIDENT_NUMBER, null);
    tokens.put(l1Prefix + GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_LOCATION, null);
    tokens.put(l1Prefix + GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_NOTIFICATION, null);
    tokens.put(l1Prefix + GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_INCIDENT_DATE, null);
    tokens.put(l1Prefix + GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_TAG_COUNT, null);
    tokens.put(l1Prefix + GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_MONIKER_CLASS, null);
    tokens.put(l1Prefix + GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_SQ_FT, null);
    tokens.put(l1Prefix + GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_PROP_TYPE, null);
    tokens.put(l1Prefix + GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_WAIVER, null);


    if (obj.has(field) && !obj.isNull(field)) {
      JSONObject inobj = obj.getJSONObject(field);

      ArrayList<String> inobjStringFields = new ArrayList<>(Arrays.asList(
          GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_INCIDENT_NUMBER,
          GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_LOCATION,
          GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_NOTIFICATION,
          GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_INCIDENT_DATE,
          GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_MONIKER_CLASS,
          GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_PROP_TYPE,
          GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_WAIVER));

      for (String infield : inobjStringFields) {
        if (inobj.has(infield) && !inobj.isNull(infield)) {
          String key = field + GEO_SYSTEMFIELD_DELIMITER + infield;
          tokens.put(key, inobj.getString(infield));
        }
        //integer
        ArrayList<String> intFields = new ArrayList<>(Arrays.asList(GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_OBJECTID,
            GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_TAG_COUNT, GEO_FIELD_INCIDENTS_PROPERTIES_OBJ_SQ_FT));

        for (String intfield : intFields) {
          if (inobj.has(intfield) && !inobj.isNull(intfield)) {
            String key = field + GEO_SYSTEMFIELD_DELIMITER + intfield;
            tokens.put(key, String.valueOf(inobj.getInt(intfield)));
          }
        }
      }
    }

    //geospatial objects
    String geoField = GEO_FIELD_INCIDENTS_GEOMETRY;

    String lPrefix = geoField + GEO_SYSTEMFIELD_DELIMITER;
    tokens.put(lPrefix + GEO_FIELD_INCIDENTS_GEOMETRY_OBJ_TYPE, null);
    tokens.put(lPrefix + GEO_FIELD_INCIDENTS_GEOMETRY_OBJ_COORDINATES, null);


    if (obj.has(geoField) && !obj.isNull(geoField)) {
      JSONObject ingobj = obj.getJSONObject(geoField);

      ArrayList<String> ingeoobjStringFields = new ArrayList<>(Arrays.asList(
          GEO_FIELD_INCIDENTS_GEOMETRY_OBJ_TYPE));

      for (String gfield : ingeoobjStringFields) {
        if (ingobj.has(gfield) && !ingobj.isNull(gfield)) {
          String key = geoField + GEO_SYSTEMFIELD_DELIMITER + gfield;
          tokens.put(key, ingobj.getString(gfield));
        }
      }

      String coord = GEO_FIELD_INCIDENTS_GEOMETRY_OBJ_COORDINATES;
      JSONArray arr = ingobj.getJSONArray(coord);
      if (arr.length() > 0) {
        String key = geoField + GEO_SYSTEMFIELD_DELIMITER + coord;
        tokens.put(key, arr.getLong(0)+","+arr.getLong(1));
      }
    }
  }

  private int getStoredCustomersCount() {
    if (storedDocsCountCustomer == 0) {
      storedDocsCountCustomer = Integer.parseInt(getVal(SOE_DOCUMENT_PREFIX_CUSTOMER + SOE_SYSTEMFIELD_DELIMITER +
          SOE_SYSTEMFIELD_STORAGEDOCS_COUNT_CUSTOMER));
    }
    return storedDocsCountCustomer;
  }

  private int getStoredIncidentsCount() {
    if (storedDocsCountIncidents == 0) {
      storedDocsCountIncidents = Integer.parseInt(getVal(GEO_DOCUMENT_PREFIX_INCIDENTS + GEO_SYSTEMFIELD_DELIMITER +
          GEO_SYSTEMFIELD_STORAGEDOCS_COUNT_INCIDENTS));
    }
    return storedDocsCountIncidents;
  }

  private int getTotalcustomersCount() {
    if (totalDocsCount == 0) {
      totalDocsCount = Integer.parseInt(getVal(SOE_DOCUMENT_PREFIX_CUSTOMER + SOE_SYSTEMFIELD_DELIMITER +
          SOE_SYSTEMFIELD_TOTALDOCS_COUNT));
    }
    return totalDocsCount;
  }

  private int getTotalIncidentsCount() {
    if (totalDocsCount == 0) {
      totalDocsCount = Integer.parseInt(getVal(GEO_DOCUMENT_PREFIX_INCIDENTS + GEO_SYSTEMFIELD_DELIMITER +
          GEO_SYSTEMFIELD_TOTALDOCS_COUNT));
    }
    return totalDocsCount;
  }

  private int getStoredOrdersCount(){
    if (storedDocsCountOrder == 0) {
      storedDocsCountOrder = Integer.parseInt(getVal(SOE_DOCUMENT_PREFIX_ORDER + SOE_SYSTEMFIELD_DELIMITER +
          SOE_SYSTEMFIELD_STORAGEDOCS_COUNT_ORDER));
    }
    return storedDocsCountOrder;
  }



  private int getNumberZipfianUnifrom() {
    if (zipfianGenerator == null) {
      zipfianGenerator = new ZipfianGenerator(1L, Long.valueOf(getStoredCustomersCount()-1).longValue());
    }
    return  zipfianGenerator.nextValue().intValue();
  }


  //getting latest docId shifted back on (max limit + max offest) to ensure the query returns expected amount of results
  private int getNumberZipfianLatests(int totalItems) {
    if (zipfianGenerator == null) {
      zipfianGenerator = new ZipfianGenerator(1L, Long.valueOf(getStoredCustomersCount()-1).longValue());
    }
    return  totalItems - zipfianGenerator.nextValue().intValue() - queryLimitMax - queryOffsetMax;
  }

  private int getNumberRandom(int limit) {
    return rand.nextInt(limit);
  }

}
