package com.yahoo.ycsb.generator.soe;

import com.yahoo.ycsb.workloads.soe.SoeQueryPredicate;
import javafx.util.Pair;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.lang.reflect.*;
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


  protected abstract void setVal(String key, String value);

  protected abstract String getVal(String key);

  protected abstract int increment(String key, int step);

  public void putCustomerDocument(String docKey, String docBody) {
    ArrayList<Pair<String, String>> tokens = tokenize(docBody);
    String prefix = SOE_DOCUMENT_PREFIX_CUSTOMER + SOE_SYSTEMFIELD_DELIMITER;
    int storageCount = increment(prefix + SOE_SYSTEMFIELD_STORAGEDOCS_COUNT, 1);

    setVal(prefix + SOE_METAFIELD_DOCID + SOE_SYSTEMFIELD_DELIMITER + storageCount, docKey);
    setVal(prefix + SOE_METAFIELD_INSERTDOC + SOE_SYSTEMFIELD_DELIMITER + storageCount, docBody);

    for (Pair<String, String> token : tokens) {
      String key = prefix + token.getKey() + SOE_SYSTEMFIELD_DELIMITER + storageCount;
      setVal(key, token.getValue());
    }
/*
    // making sure all fields are initialized
    Field[] fields = Generator.class.getDeclaredFields();
    for (Field f: fields) {
      if (f.getName().startsWith("SOE_FIELD_CUSTOMER")) {
        String entryKey = prefix + f.getName() + SOE_SYSTEMFIELD_DELIMITER + storageCount;
        try {
          getVal(entryKey);
        } catch (Exception e) {
          if (storageCount > 1) {
            for(int i = storageCount-1; i > 0; i--) {
              try {
                String entryKeyPrev = prefix + f.getName() + SOE_SYSTEMFIELD_DELIMITER + i;
                String v = getVal(entryKeyPrev);
                setVal(entryKey, v);
                break;
              } catch (Exception ex) {
                continue;
              }
            }
          }
        }
        int firstValidValueCursor = 0;
        String firstValidValue = "";
        for (int i = 1; i < storageCount; i++) {
          entryKey = prefix + f.getName() + SOE_SYSTEMFIELD_DELIMITER + i;
          try {
            firstValidValue = getVal(entryKey);
            firstValidValueCursor = i;
            break;
          } catch (Exception ex) {
            continue;
          }
        }
        for (int i = 1; i < firstValidValueCursor; i++) {
          entryKey = prefix + f.getName() + SOE_SYSTEMFIELD_DELIMITER + i;
          setVal(entryKey, firstValidValue);
        }
      }
    }
    */

    /*
    Field[] fields = Generator.class.getDeclaredFields();
    int validValueCursor = 0;
    for (Field f: fields) {
      if (f.getName().startsWith("SOE_FIELD_CUSTOMER")) {
        for (int i = 1; i< storageCount; i++) {
          String entryKey = prefix + f.getName() + SOE_SYSTEMFIELD_DELIMITER + i;
          try {
            getVal(entryKey);
            validValueCursor = i;
          } catch (Exception e) {
            try {
              int prev = i-1;
              String entryKeyPrev = prefix + f.getName() + SOE_SYSTEMFIELD_DELIMITER + prev;
              String v = getVal(entryKeyPrev);
              setVal(entryKey, v);
              validValueCursor = i;
            } catch (Exception e1) {
              continue;
            }
          }
        }
        for (int i = validValueCursor; i > 1; i--) {
          String entryKey = prefix + f.getName() + SOE_SYSTEMFIELD_DELIMITER + i;
          try {
            getVal(entryKey);
          } catch (Exception e) {
            try {
              int next = i+1;
              String entryKeyNext = prefix + f.getName() + SOE_SYSTEMFIELD_DELIMITER + next;
              String v = getVal(entryKeyNext);
              setVal(entryKey, v);
            } catch (Exception e1) {
              continue;
            }
          }
        }
      }
    }
    */

  }

  public Pair<String, String> getInserDocument() {
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
    int docCounter = increment(SOE_SYSTEMFIELD_INSERTDOC_COUNTER, 1);
    String docKey = SOE_DOCUMENT_PREFIX_CUSTOMER + SOE_SYSTEMFIELD_DELIMITER + docCounter;
    insertDocument = new Pair<String, String>(docKey, docBody);
  }


  public void buildUpdatePredicate() {
    soePredicate = new SoeQueryPredicate();
    soePredicate.setName(SOE_FIELD_CUSTOMER_BALLANCE);
    soePredicate.setValueA("$" + rand.nextInt(99999) + "." + rand.nextInt(99));
    soePredicate.setDocid(getVal(buildStorageKey(SOE_DOCUMENT_PREFIX_CUSTOMER, SOE_METAFIELD_DOCID)));
  }


  public void buildReadPredicate() {
    soePredicate = new SoeQueryPredicate();
    soePredicate.setDocid(getVal(buildStorageKey(SOE_DOCUMENT_PREFIX_CUSTOMER, SOE_METAFIELD_DOCID)));
  }


  public void buildScanPredicate() {
    soePredicate = new SoeQueryPredicate();
    soePredicate.setDocid(getVal(buildStorageKey(SOE_DOCUMENT_PREFIX_CUSTOMER, SOE_METAFIELD_DOCID)));
  }


  public void buildPagePredicate() {
    soePredicate.setName(SOE_FIELD_CUSTOMER_ADDRESS);

    soePredicate.setNestedPredicateA(new SoeQueryPredicate());
    soePredicate.getNestedPredicateA().setName(SOE_FIELD_CUSTOMER_ADDRESS_OBJ_ZIP);
    soePredicate.getNestedPredicateA().setValueA(getVal(buildStorageKey(SOE_DOCUMENT_PREFIX_CUSTOMER,
        SOE_FIELD_CUSTOMER_ADDRESS,
        SOE_FIELD_CUSTOMER_ADDRESS_OBJ_ZIP)));
    soePredicate.getNestedPredicateA().setType(SoeQueryPredicate.SOE_PREDICATE_TYPE_INTEGER);
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


  private ArrayList<Pair<String, String>> tokenize(String jsonString) {
    ArrayList<Pair<String, String>> tokens = new ArrayList<>();
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


  private void tokenizeFields(JSONObject obj, ArrayList<Pair<String, String>> tokens) {

    //string
    ArrayList<String> stringFields = new ArrayList<>(Arrays.asList(SOE_FIELD_CUSTOMER_ID,
        SOE_FIELD_CUSTOMER_GID, SOE_FIELD_CUSTOMER_FNAME, SOE_FIELD_CUSTOMER_LNAME, SOE_FIELD_CUSTOMER_MNAME,
        SOE_FIELD_CUSTOMER_BALLANCE, SOE_FIELD_CUSTOMER_DOB, SOE_FIELD_CUSTOMER_EMAIL, SOE_FIELD_CUSTOMER_PHONECOUNTRY,
        SOE_FIELD_CUSTOMER_PHONE, SOE_FIELD_CUSTOMER_AGEGROUP, SOE_FIELD_CUSTOMER_URLPROTOCOL,
        SOE_FIELD_CUSTOMER_URLSITE, SOE_FIELD_CUSTOMER_URLDOMAIN, SOE_FIELD_CUSTOMER_URL));
    for (String field : stringFields) {
      if (obj.has(field) && !obj.isNull(field)) {
        tokens.add(new Pair<String, String>(field, obj.getString(field)));
      }
    }

    //integer
    ArrayList<String> intFields = new ArrayList<>(Arrays.asList(SOE_FIELD_CUSTOMER_DOCID,
        SOE_FIELD_CUSTOMER_LINEARSCORE, SOE_FIELD_CUSTOMER_WEIGHTEDSCORE, SOE_FIELD_CUSTOMER_AGE));
    for (String field : intFields) {
      if (obj.has(field) && !obj.isNull(field)) {
        tokens.add(new Pair<String, String>(field, String.valueOf(obj.getInt(field))));
      }
    }

    //boolean
    String field = SOE_FIELD_CUSTOMER_ISACTIVE;
    if (obj.has(field) && !obj.isNull(field)) {
      tokens.add(new Pair<String, String>(field, String.valueOf(obj.getBoolean(field))));
    }


  }

  private void tokenizeArrays(JSONObject obj, ArrayList<Pair<String, String>> tokens) {

    //array
    String field = SOE_FIELD_CUSTOMER_DEVICES;
    if (obj.has(field) && !obj.isNull(field)) {
      JSONArray arr = obj.getJSONArray(field);
      if (arr.length() > 0) {
        int element = rand.nextInt(arr.length());
        tokens.add(new Pair<String, String>(field, arr.getString(element)));
      }
    }

    //array of arrays
    field = SOE_FIELD_CUSTOMER_LINKEDDEVICES;
    if (obj.has(field) && !obj.isNull(field)) {
      JSONArray arr = obj.getJSONArray(field);
      if (arr.length() > 0) {
        int element = rand.nextInt(arr.length());
        JSONArray inarr = arr.getJSONArray(element);
        if (inarr.length() > 0) {
          int inelement = rand.nextInt(inarr.length());
          tokens.add(new Pair<String, String>(field, inarr.getString(inelement)));
        }
      }
    }

    //array of objects
    field = SOE_FIELD_CUSTOMER_CHILDREN;
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
            tokens.add(new Pair<String, String>(key, inobj.getString(infield)));
          }
        }
        String infield = SOE_FIELD_CUSTOMER_CHILDREN_OBJ_AGE;
        if (inobj.has(infield) && !inobj.isNull(infield)) {
          String key = field + SOE_SYSTEMFIELD_DELIMITER + infield;
          tokens.add(new Pair<String, String>(key, String.valueOf(inobj.getInt(infield))));
        }
      }
    }

    //array of objects with array
    field = SOE_FIELD_CUSTOMER_VISITEDPLACES;
    if (obj.has(field) && !obj.isNull(field)) {
      JSONArray inarr = obj.getJSONArray(field);
      if (inarr.length()>0) {
        int element = rand.nextInt(inarr.length());
        JSONObject inobj = inarr.getJSONObject(element);
        String infield = SOE_FIELD_CUSTOMER_VISITEDPLACES_OBJ_COUNTRY;
        if (inobj.has(infield) && !inobj.isNull(infield)) {
          String key = field + SOE_SYSTEMFIELD_DELIMITER + infield;
          tokens.add(new Pair<String, String>(key, inobj.getString(infield)));
        }
        infield = SOE_FIELD_CUSTOMER_VISITEDPLACES_OBJ_CITIES;
        if (inobj.has(infield) && !inobj.isNull(infield)) {
          JSONArray inarr2 = inobj.getJSONArray(infield);
          if (inarr2.length() >0) {
            int inelement2 = rand.nextInt(inarr2.length());
            String key = field + SOE_SYSTEMFIELD_DELIMITER + infield;
            tokens.add(new Pair<String, String>(key, inarr2.getString(inelement2)));
          }
        }
      }
    }
  }

  private void tokenizeObjects(JSONObject obj, ArrayList<Pair<String, String>> tokens) {

    //3-level nested objects
    String field = SOE_FIELD_CUSTOMER_ADDRESS;
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
          tokens.add(new Pair<String, String>(key, inobj.getString(infield)));
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
            tokens.add(new Pair<String, String>(key, inobj2.getString(infield2)));
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
              tokens.add(new Pair<String, String>(key, inobj3.getString(infield3)));
            }
          }
        }
      }
    }
  }
}
