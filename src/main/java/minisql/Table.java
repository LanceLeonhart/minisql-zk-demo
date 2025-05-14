package minisql;

import java.util.*;

/**
 * 内存表：列定义、主键列、行数据（主键值 → 行内容）
 */
public class Table {
    private final String name;
    private final List<Column> columns;
    private final String primaryKey;
    private final Map<String, Map<String, String>> rows;

    public Table(String name, List<Column> columns, String primaryKey) {
        this.name       = name;
        this.columns    = new ArrayList<>(columns);
        this.primaryKey = primaryKey;
        if (columns.stream().noneMatch(c -> c.getName().equals(primaryKey))) {
            throw new IllegalArgumentException("Primary key must be one of columns");
        }
        this.rows       = new LinkedHashMap<>();
    }

    public String getName() {
        return name;
    }

    public List<Column> getColumns() {
        return Collections.unmodifiableList(columns);
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    /**
     * 插入一行：检查列名、类型、主键唯一
     */
    public String insertRow(Map<String, String> row) {
        // 1. 列名检查
        Set<String> expected = new HashSet<>();
        for (Column c : columns) expected.add(c.getName());
        if (!row.keySet().equals(expected)) {
            return "Column names mismatch. Expected: " + expected;
        }
        // 2. 类型检查
        for (Column c : columns) {
            String val = row.get(c.getName());
            switch (c.getType()) {
                case "INT":
                    try {
                        Integer.parseInt(val);
                    } catch (NumberFormatException e) {
                        return "Type error: column `" + c.getName() + "` expects INT";
                    }
                    break;
                case "TEXT":
                    // always ok
                    break;
                default:
                    return "Unknown type: " + c.getType();
            }
        }
        // 3. 主键唯一
        String pkVal = row.get(primaryKey);
        if (rows.containsKey(pkVal)) {
            return "Duplicate primary key: " + pkVal;
        }
        rows.put(pkVal, new LinkedHashMap<>(row));
        return "OK";
    }

    /** 查询所有行 */
    public List<Map<String, String>> selectAll() {
        return new ArrayList<>(rows.values());
    }

    /** 按主键查询单行 */
    public Map<String, String> selectByKey(String key) {
        return rows.get(key);
    }

    /** 删除单行（按主键） */
    public String deleteByKey(String key) {
        return rows.remove(key) != null ? "OK" : "NOT FOUND";
    }
}
