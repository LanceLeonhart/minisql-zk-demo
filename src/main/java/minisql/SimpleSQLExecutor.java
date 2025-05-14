package minisql;

import java.util.*;
import java.util.regex.*;

/**
 * 简易 SQL 执行器：支持 CREATE, DROP, INSERT, SELECT, DELETE, UPDATE
 */
public class SimpleSQLExecutor {

    public static String execute(String sql) {
        sql = sql.trim();
        String up = sql.toUpperCase(Locale.ROOT);
        if (up.startsWith("CREATE TABLE")) {
            return handleCreate(sql);
        } else if (up.startsWith("DROP TABLE")) {
            return handleDrop(sql);
        } else if (up.startsWith("INSERT INTO")) {
            return handleInsert(sql);
        } else if (up.startsWith("SELECT")) {
            return handleSelect(sql);
        } else if (up.startsWith("DELETE FROM")) {
            return handleDelete(sql);
        } else if (up.startsWith("UPDATE")) {
            return handleUpdate(sql);
        }
        return "Unsupported SQL.";
    }

    // CREATE TABLE users (id INT, name TEXT, PRIMARY KEY(id))
    private static String handleCreate(String sql) {
        Pattern p = Pattern.compile(
                "CREATE TABLE (\\w+) \\((.+),\\s*PRIMARY KEY\\((\\w+)\\)\\)",
                Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(sql);
        if (!m.find()) return "Invalid CREATE syntax.";
        String tableName = m.group(1);
        String colsDef   = m.group(2).trim();
        String pk        = m.group(3);

        List<Column> cols = new ArrayList<>();
        for (String part : colsDef.split(",")) {
            String[] kv = part.trim().split("\\s+");
            if (kv.length < 2) return "Invalid column definition: " + part;
            cols.add(new Column(kv[0], kv[1]));
        }
        boolean ok = TableManager.createTable(tableName, cols, pk);
        return ok ? "Table created: " + tableName
                : "Table already exists: " + tableName;
    }

    // DROP TABLE users
    private static String handleDrop(String sql) {
        Pattern p = Pattern.compile("DROP TABLE (\\w+)", Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(sql);
        if (!m.find()) return "Invalid DROP syntax.";
        String tableName = m.group(1);
        boolean ok = TableManager.dropTable(tableName);
        return ok ? "Table dropped: " + tableName
                : "Table not found: " + tableName;
    }

    // INSERT INTO users (id, name) VALUES (1, 'Alice')
    private static String handleInsert(String sql) {
        Pattern p = Pattern.compile(
                "INSERT INTO (\\w+) \\(([^)]+)\\) VALUES \\(([^)]+)\\)",
                Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(sql);
        if (!m.find()) return "Invalid INSERT syntax.";
        String tableName = m.group(1);
        String colsPart  = m.group(2);
        String valsPart  = m.group(3);

        Table table = TableManager.getTable(tableName);
        if (table == null) return "Table not found: " + tableName;

        List<String> cols = Arrays.stream(colsPart.split(","))
                .map(String::trim).toList();
        List<String> vals = Arrays.stream(valsPart.split(","))
                .map(String::trim)
                .map(s -> s.replaceAll("^'(.*)'$", "$1"))
                .toList();

        if (cols.size() != vals.size()) return "Column/value count mismatch.";

        // 使用 LinkedHashMap 保持插入顺序
        Map<String, String> row = new LinkedHashMap<>();
        for (int i = 0; i < cols.size(); i++) {
            row.put(cols.get(i), vals.get(i));
        }
        return table.insertRow(row);
    }

    // SELECT * FROM users [WHERE col = val]
    private static String handleSelect(String sql) {
        // WHERE 条件
        Pattern pWhere = Pattern.compile(
                "SELECT \\* FROM (\\w+) WHERE (\\w+)\\s*=\\s*('?\\w+'?)",
                Pattern.CASE_INSENSITIVE);
        Matcher mWhere = pWhere.matcher(sql);
        if (mWhere.find()) {
            String tableName = mWhere.group(1);
            String col       = mWhere.group(2);
            String val       = mWhere.group(3).replaceAll("'", "");

            Table table = TableManager.getTable(tableName);
            if (table == null) return "Table not found: " + tableName;
            List<Map<String, String>> rows = table.selectWhere(col, val);
            if (rows.isEmpty()) return "Empty result.";
            StringBuilder sb = new StringBuilder();
            for (Map<String, String> r : rows) {
                sb.append(r).append("\n");
            }
            return sb.toString().trim();
        }

        // 全表查询
        Pattern pAll = Pattern.compile("SELECT \\* FROM (\\w+)", Pattern.CASE_INSENSITIVE);
        Matcher mAll = pAll.matcher(sql);
        if (mAll.find()) {
            String tableName = mAll.group(1);
            Table table = TableManager.getTable(tableName);
            if (table == null) return "Table not found: " + tableName;
            List<Map<String, String>> all = table.selectAll();
            if (all.isEmpty()) return "Empty table.";
            StringBuilder sb = new StringBuilder();
            for (Map<String, String> r : all) {
                sb.append(r).append("\n");
            }
            return sb.toString().trim();
        }

        return "Invalid SELECT syntax.";
    }

    // DELETE FROM users [WHERE col = val]
    private static String handleDelete(String sql) {
        Pattern p = Pattern.compile(
                "DELETE FROM (\\w+)(?: WHERE (\\w+)\\s*=\\s*('?\\w+'?))?",
                Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(sql);
        if (!m.find()) return "Invalid DELETE syntax.";
        String tableName = m.group(1);
        String col       = m.group(2);
        String rawVal    = m.group(3);

        Table table = TableManager.getTable(tableName);
        if (table == null) return "Table not found: " + tableName;

        if (col != null) {
            String val = rawVal.replaceAll("'", "");
            int cnt = table.deleteWhere(col, val);
            return "Deleted rows: " + cnt;
        }
        // 不带 WHERE 则清空整表
        Set<Column> cols = new LinkedHashSet<>(table.getColumns());
        String pk = table.getPrimaryKey();
        TableManager.dropTable(tableName);
        // 重建一个空表
        TableManager.createTable(tableName, new ArrayList<>(cols), pk);
        return "Table cleared: " + tableName;
    }

    // UPDATE users SET col1=val1 [, col2=val2...] WHERE col=val
    private static String handleUpdate(String sql) {
        Pattern p = Pattern.compile(
                "UPDATE (\\w+) SET (.+?) WHERE (\\w+)\\s*=\\s*('?\\w+'?)",
                Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(sql);
        if (!m.find()) return "Invalid UPDATE syntax.";
        String tableName = m.group(1);
        String setPart   = m.group(2);
        String colCond   = m.group(3);
        String rawVal    = m.group(4).replaceAll("'", "");

        Table table = TableManager.getTable(tableName);
        if (table == null) return "Table not found: " + tableName;

        Map<String, String> newValues = new HashMap<>();
        for (String assign : setPart.split(",")) {
            String[] kv = assign.trim().split("=");
            newValues.put(kv[0].trim(), kv[1].trim().replaceAll("'", ""));
        }

        int updated = table.updateWhere(colCond, rawVal, newValues);
        if (updated < 0) return "Update error (type or column mismatch).";
        return "Updated rows: " + updated;
    }
}
