package minisql;

import java.util.*;

/**
 * 内存表：列定义、主键列、行数据（主键值 → 行内容）
 */
public class Table {
    private final String name;
    private final List<Column> columns;
    private final String primaryKey;
    // 主键值 → (列名→列值)
    private final Map<String, Map<String, String>> rows;

    public Table(String name, List<Column> columns, String primaryKey) {
        this.name       = name;
        this.columns    = new ArrayList<>(columns);
        this.primaryKey = primaryKey;
        if (columns.stream().noneMatch(c -> c.getName().equals(primaryKey))) {
            throw new IllegalArgumentException("Primary key must be one of columns");
        }
        this.rows = new LinkedHashMap<>();
    }

    public String getName() { return name; }
    public List<Column> getColumns() { return Collections.unmodifiableList(columns); }
    public String getPrimaryKey() { return primaryKey; }

    /** 插入一行：列名检查、类型检查、主键唯一 */
    public String insertRow(Map<String, String> row) {
        Set<String> expectedCols = new HashSet<>();
        for (Column c : columns) expectedCols.add(c.getName());
        if (!row.keySet().equals(expectedCols)) {
            return "Column names mismatch. Expected: " + expectedCols;
        }
        // 类型检查
        for (Column c : columns) {
            String val = row.get(c.getName());
            switch (c.getType()) {
                case "INT":
                    try { Integer.parseInt(val); }
                    catch (NumberFormatException e) {
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
        String pkVal = row.get(primaryKey);
        if (rows.containsKey(pkVal)) {
            return "Duplicate primary key: " + pkVal;
        }
        // 深拷贝存储
        rows.put(pkVal, new LinkedHashMap<>(row));
        return "OK";
    }

    /** 查询所有行 */
    public List<Map<String, String>> selectAll() {
        return new ArrayList<>(rows.values());
    }

    /** 按任意列查询（返回匹配的所有行） */
    public List<Map<String, String>> selectWhere(String col, String val) {
        List<Map<String, String>> result = new ArrayList<>();
        for (Map<String, String> row : rows.values()) {
            if (val.equals(row.get(col))) {
                result.add(new LinkedHashMap<>(row));
            }
        }
        return result;
    }

    /** 删除单行（按主键） */
    public String deleteByKey(String key) {
        return rows.remove(key) != null ? "OK" : "NOT FOUND";
    }

    /** 删除多行（按任意列） */
    public int deleteWhere(String col, String val) {
        List<String> toRemove = new ArrayList<>();
        for (Map.Entry<String, Map<String, String>> e : rows.entrySet()) {
            if (val.equals(e.getValue().get(col))) {
                toRemove.add(e.getKey());
            }
        }
        for (String k : toRemove) rows.remove(k);
        return toRemove.size();
    }

    /** 更新行（按任意列） */
    public int updateWhere(String colCond, String valCond, Map<String, String> newValues) {
        // 类型 & 列名校验 for newValues
        Set<String> validCols = new HashSet<>();
        for (Column c : columns) validCols.add(c.getName());
        if (!validCols.containsAll(newValues.keySet())) return -1;

        for (Column c : columns) {
            if (newValues.containsKey(c.getName())) {
                String v = newValues.get(c.getName());
                if ("INT".equals(c.getType())) {
                    try { Integer.parseInt(v); }
                    catch (NumberFormatException e) { return -2; }
                }
            }
        }

        int count = 0;
        for (Map<String, String> row : rows.values()) {
            if (valCond.equals(row.get(colCond))) {
                row.putAll(newValues);
                count++;
            }
        }
        return count;
    }
}
