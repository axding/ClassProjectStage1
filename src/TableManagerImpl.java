import java.util.HashMap;
import java.util.List;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
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
  private HashMap<String, TableMetadata> tables = new HashMap<>();

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

    DirectorySubspace newTable = rootDirectory.create(db, PathUtil.from(tableName)).join();
    TableMetadata metadata = new TableMetadata(attributeNames, attributeType, primaryKeyAttributeNames);
    tables.put(tableName, metadata);

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
    tables.remove(tableName);

    return StatusCode.SUCCESS;
  }

  @Override
  public HashMap<String, TableMetadata> listTables() {
    // your code
    return tables;
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

    TableMetadata tableMetadata = tables.get(tableName);
    if (tableMetadata.doesAttributeExist(attributeName)) {
      return StatusCode.ATTRIBUTE_ALREADY_EXISTS;
    }
    
    tableMetadata.addAttribute(attributeName, attributeType);

    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode dropAttribute(String tableName, String attributeName) {
    // your code
    boolean tableExists = rootDirectory.exists(db, PathUtil.from(tableName)).join();
    if (!tableExists) {
      return StatusCode.TABLE_NOT_FOUND;
    }

    TableMetadata tableMetadata = tables.get(tableName);
    if (!tableMetadata.doesAttributeExist(attributeName)) {
      return StatusCode.ATTRIBUTE_NOT_FOUND;
    }

    HashMap<String, AttributeType> attributes = tableMetadata.getAttributes();
    attributes.remove(attributeName);
    tableMetadata.setAttributes(attributes);

    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode dropAllTables() {
    // your code
    List<String> subdirectories = DirectoryLayer.getDefault().list(db, PathUtil.from("root")).join();
    for (String subdirectory : subdirectories) {
      rootDirectory.remove(db, PathUtil.from(subdirectory)).join();
    }
    tables.clear();
    return StatusCode.SUCCESS;
  }
}
