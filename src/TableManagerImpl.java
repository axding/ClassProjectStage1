import java.util.HashMap;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.PathUtil;
import com.apple.foundationdb.tuple.Tuple;

/**
 * TableManagerImpl implements interfaces in {#TableManager}. You should put your implementation
 * in this class.
 */
public class TableManagerImpl implements TableManager{
  private static final FDB fdb;
  private static final Database db;
  private static final DirectorySubspace rootDirectory;

  static {
    fdb = FDB.selectAPIVersion(710);
    db = fdb.open();
    rootDirectory = DirectoryLayer.getDefault().createOrOpen(db, PathUtil.from("root")).join();
  }

  @Override
  public StatusCode createTable(String tableName, String[] attributeNames, AttributeType[] attributeType,
                         String[] primaryKeyAttributeNames) {

    // check for correct attributes
    if (attributeNames == null || attributeType == null) {
      return StatusCode.TABLE_CREATION_ATTRIBUTE_INVALID;
    }
    else if (attributeNames.length != attributeType.length) {
      return StatusCode.TABLE_CREATION_ATTRIBUTE_INVALID;
    }

    if (primaryKeyAttributeNames == null) {
      return StatusCode.TABLE_CREATION_NO_PRIMARY_KEY;
    }

    for (String PKAttributeName : primaryKeyAttributeNames) {
      boolean foundAttribute = false;
      for (String attributeName : attributeNames) {
        if (PKAttributeName.equals(attributeName)) {
          foundAttribute = true;
        }
      }

      if (!foundAttribute) {
        return StatusCode.TABLE_CREATION_PRIMARY_KEY_NOT_FOUND;
      }
    }

    for (AttributeType type : attributeType) {
      if (type != AttributeType.INT && type != AttributeType.VARCHAR && type != AttributeType.DOUBLE) {
        return StatusCode.ATTRIBUTE_TYPE_NOT_SUPPORTED;
      }
    }

    // check if the table exists
    boolean tableExists = rootDirectory.exists(db, PathUtil.from(tableName)).join();
    if (tableExists) {
      return StatusCode.TABLE_ALREADY_EXISTS;
    }

    // create table
    DirectorySubspace newTable = rootDirectory.create(db, PathUtil.from(tableName)).join();
    Transaction tx = db.createTransaction();

    Tuple attributeKey = new Tuple();
    attributeKey = attributeKey.add("attributes");
    Tuple attributeValues = new Tuple();

    for (int i = 0; i < attributeNames.length; i++) {
      String attributeName = attributeNames[i];
      AttributeType type = attributeType[i];
      boolean isPK = false;

      for (String PKAttributeName : primaryKeyAttributeNames) {
        if (PKAttributeName.equals(attributeName)) {
          isPK = true;
        }
      }

      Tuple keyTuple = new Tuple();
      keyTuple = keyTuple.add(attributeName);

      Tuple valueTuple = new Tuple();
      String typeString = "";
      if (type == AttributeType.INT) {
        typeString = "INT";
      }
      else if (type == AttributeType.VARCHAR) {
        typeString = "VARCHAR";
      }
      else {
        typeString = "DOUBLE";
      }
      valueTuple = valueTuple.add(typeString).add(isPK);
      tx.set(newTable.pack(keyTuple), valueTuple.pack());

      attributeValues = attributeValues.add(attributeName);
    }
    tx.set(newTable.pack(attributeKey), attributeValues.pack());
    tx.commit().join();
    tx.close();

    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode deleteTable(String tableName) {
    // your code
    boolean tableExists = rootDirectory.exists(db, PathUtil.from(tableName)).join();
    if (!tableExists) {
      return StatusCode.TABLE_NOT_FOUND;
    }

    rootDirectory.remove(db, PathUtil.from(tableName)).join();

    return StatusCode.SUCCESS;
  }

  @Override
  public HashMap<String, TableMetadata> listTables() {
    // your code
    HashMap<String, TableMetadata> allTables = new HashMap<>();

    List<String> tableNames = DirectoryLayer.getDefault().list(db, PathUtil.from("root")).join();

    for (String tableName : tableNames) {
      Transaction tx = db.createTransaction();
      DirectorySubspace table = rootDirectory.open(db, PathUtil.from(tableName), rootDirectory.getLayer()).join();
      
      // obtain list of attribute names
      Tuple attributes = null;
      try {
        Tuple keyTuple = new Tuple();
        keyTuple = keyTuple.add("attributes");
        attributes = Tuple.fromBytes(tx.get(table.pack(keyTuple)).get());
        
      } catch (Exception e) {
          System.out.println("ERROR when fetching attribute names: " + e);
          return null;
      }

      List<String> attributeNames = new ArrayList<String>();
      List<AttributeType> attributeTypes = new ArrayList<AttributeType>();
      List<String> pkAttributes = new ArrayList<String>();
      

      // find data for each attribute
      // System.out.println("outside loop");
      for (int i = 0; i < attributes.size(); i++) {
        String key = attributes.getString(i);
        String typeValue = "INT";
        boolean isPKValue = true;
        try {
          // System.out.println(key);
          Tuple keyTuple = new Tuple();
          keyTuple = keyTuple.add(key);
          Tuple values = Tuple.fromBytes(tx.get(table.pack(keyTuple)).get());
          typeValue = values.getString(0);
          isPKValue = values.getBoolean(1);

        } catch (Exception e) {
            System.out.println("ERROR when querying attribute data: " + e);
            return null;
        }

        AttributeType type = null;
        if (typeValue.equals("INT")) {
          type = AttributeType.INT;
        } else if (typeValue.equals("VARCHAR")) {
          type = AttributeType.VARCHAR;
        }
        else {
          type = AttributeType.DOUBLE;
        }

        // System.out.println(key);
        // System.out.println(type);
        // System.out.println(isPKValue);

        attributeNames.add(key);
        attributeTypes.add(type);
        if (isPKValue) {
          pkAttributes.add(key);
        }
      }

      // System.out.println(Arrays.toString(attributeNames.toArray()));
      // System.out.println(Arrays.toString(attributeTypes.toArray()));
      // System.out.println(Arrays.toString(pkAttributes.toArray()));

      tx.commit().join();
      tx.close();
      TableMetadata metadata = new TableMetadata(attributeNames.toArray(new String[0]), attributeTypes.toArray(new AttributeType[0]), pkAttributes.toArray(new String[0]));
      allTables.put(tableName, metadata);
    }

    return allTables;
  }

  @Override
  public StatusCode addAttribute(String tableName, String attributeName, AttributeType attributeType) {
    // your code
    boolean tableExists = rootDirectory.exists(db, PathUtil.from(tableName)).join();
    if (!tableExists) {
      return StatusCode.TABLE_NOT_FOUND;
    }

    if (attributeType != AttributeType.INT && attributeType != AttributeType.VARCHAR && attributeType != AttributeType.DOUBLE) {
      return StatusCode.ATTRIBUTE_TYPE_NOT_SUPPORTED;
    }


    // check if attribute exists
    Transaction tx = db.createTransaction();
    DirectorySubspace table = rootDirectory.open(db, PathUtil.from(tableName), rootDirectory.getLayer()).join();
    Tuple keyTuple = new Tuple();
    keyTuple = keyTuple.add(attributeName);
    try {
      byte[] encodedValues = tx.get(table.pack(keyTuple)).get();
      if (encodedValues != null) {
        return StatusCode.ATTRIBUTE_ALREADY_EXISTS;
      }
    } catch (Exception e) {
        System.out.println("ERROR when searching for attribute: " + e);
    }

    // add attribute
    Tuple valueTuple = new Tuple();
    String typeString = "";
    if (attributeType == AttributeType.INT) {
      typeString = "INT";
    }
    else if (attributeType == AttributeType.VARCHAR) {
      typeString = "VARCHAR";
    }
    else {
      typeString = "DOUBLE";
    }
    valueTuple = valueTuple.add(typeString).add(false);
    tx.set(table.pack(keyTuple), valueTuple.pack());

    try {
      Tuple key = new Tuple();
      key = key.add("attributes");
      Tuple attributesTuple = Tuple.fromBytes(tx.get(table.pack(key)).get());
      
      attributesTuple = attributesTuple.add(attributeName);
      tx.set(table.pack(key), attributesTuple.pack());
    } catch (Exception e) {
        System.out.println("ERROR when obtain attribute data: " + e);
    }

    tx.commit().join();
    tx.close();

    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode dropAttribute(String tableName, String attributeName) {
    // your code
    boolean tableExists = rootDirectory.exists(db, PathUtil.from(tableName)).join();
    if (!tableExists) {
      return StatusCode.TABLE_NOT_FOUND;
    }

    // check if attribute exists
    Transaction tx = db.createTransaction();
    DirectorySubspace table = rootDirectory.open(db, PathUtil.from(tableName), rootDirectory.getLayer()).join();
    Tuple keyTuple = new Tuple();
    keyTuple = keyTuple.add(attributeName);

    try {
      byte[] encodedValues = tx.get(table.pack(keyTuple)).get();
      if (encodedValues == null) {
        return StatusCode.ATTRIBUTE_NOT_FOUND;
      }
    } catch (Exception e) {
        System.out.println("ERROR when searching for attribute: " + e);
    }

    // remove attribute
    tx.clear(table.pack(keyTuple));

    try {
      Tuple key = new Tuple();
      key = key.add("attributes");
      Tuple attributesTuple = Tuple.fromBytes(tx.get(table.pack(key)).get());
      Tuple newAttributes = new Tuple();
      
      for (int i = 0; i < attributesTuple.size(); i++) {
        String attr = attributesTuple.getString(i);
        if (!attr.equals(attributeName)) {
          newAttributes = newAttributes.add(attr);
        }
      }
      tx.set(table.pack(key), newAttributes.pack());
    } catch (Exception e) {
        System.out.println("ERROR when obtain attribute data: " + e);
    }

    tx.commit().join();
    tx.close();

    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode dropAllTables() {
    // your code
    List<String> subdirectories = DirectoryLayer.getDefault().list(db, PathUtil.from("root")).join();
    for (String subdirectory : subdirectories) {
      rootDirectory.remove(db, PathUtil.from(subdirectory)).join();
    }
    return StatusCode.SUCCESS;
  }
}
