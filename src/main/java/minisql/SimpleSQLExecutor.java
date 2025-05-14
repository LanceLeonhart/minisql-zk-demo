package minisql;

import java.util.*;
import java.util.regex.*;

/**
 * 简易 SQL 执行器：支持 CREATE, DROP, INSERT, SELECT, DELETE
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
        } else {
            return "Unsupported SQL.";
        }
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

        Map<String, String> row = new HashMap<>();
        for (int i = 0; i < cols.size(); i++) {
            row.put(cols.get(i), vals.get(i));
        }
        return table.insertRow(row);
    }

    // SELECT * FROM users [WHERE id = 1]
    private static String handleSelect(String sql) {
        Pattern pPK  = Pattern.compile(
                "SELECT \\* FROM (\\w+) WHERE (\\w+)\\s*=\\s*('?\\w+'?)",
                Pattern.CASE_INSENSITIVE);
        Matcher mPK = pPK.matcher(sql);
        if (mPK.find()) {
            String tableName = mPK.group(1);
            String col       = mPK.group(2);
            String val       = mPK.group(3).replaceAll("'", "");

            Table table = TableManager.getTable(tableName);
            if (table == null) return "Table not found: " + tableName;
            if (!col.equals(table.getPrimaryKey())) {
                return "Can only filter on primary key: " + table.getPrimaryKey();
            }
            Map<String, String> row = table.selectByKey(val);
            return row == null ? "NOT FOUND" : row.toString();
        }

        Pattern pAll = Pattern.compile("SELECT \\* FROM (\\w+)", Pattern.CASE_INSENSITIVE);
        Matcher mAll = pAll.matcher(sql);
        if (mAll.find()) {
            String tableName = mAll.group(1);
            Table table = TableManager.getTable(tableName);
            if (table == null) return "Table not found: " + tableName;
            List<Map<String, String>> all = table.selectAll();
            if (all.isEmpty()) return "Empty table.";
            StringBuilder sb = new StringBuilder();
            for (Map<String, String> row : all) {
                sb.append(row).append("\n");
            }
            return sb.toString().trim();
        }
        return "Invalid SELECT syntax.";
    }

    // DELETE FROM users [WHERE id = 1]
    private static String handleDelete(String sql) {
        Pattern p = Pattern.compile(
                "DELETE FROM (\\w+)(?: WHERE (\\w+)\\s*=\\s*('?\\w+'?))?",
                Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(sql);
        if (!m.find()) return "Invalid DELETE syntax.";
        String tableName = m.group(1);
        String col       = m.group(2);
        String valRaw    = m.group(3);

        Table table = TableManager.getTable(tableName);
        if (table == null) return "Table not found: " + tableName;

        // 带 WHERE 则只删一行
        if (col != null) {
            String val = valRaw.replaceAll("'", "");
            if (!col.equals(table.getPrimaryKey())) {
                return "Can only delete by primary key: " + table.getPrimaryKey();
            }
            return table.deleteByKey(val);
        }
        // 不带 WHERE 则删整张表内容
        TableManager.dropTable(tableName);
        TableManager.createTable(tableName, table.getColumns(), table.getPrimaryKey());
        return "Table cleared: " + tableName;
    }
}
