package minisql;

import java.util.*;

/**
 * 本地表管理：创建、获取、删除
 */
public class TableManager {
    private static final Map<String, Table> tables = new HashMap<>();

    /** 创建表，返回 true 表示新建，false 表示已存在 */
    public static boolean createTable(String name, List<Column> columns, String primaryKey) {
        if (tables.containsKey(name)) return false;
        tables.put(name, new Table(name, columns, primaryKey));
        return true;
    }

    /** 删除表，返回 true 表示删除成功 */
    public static boolean dropTable(String name) {
        return tables.remove(name) != null;
    }

    /** 获取表实例或 null */
    public static Table getTable(String name) {
        return tables.get(name);
    }

    /** 列出所有表名 */
    public static Set<String> listTables() {
        return Collections.unmodifiableSet(tables.keySet());
    }
}
