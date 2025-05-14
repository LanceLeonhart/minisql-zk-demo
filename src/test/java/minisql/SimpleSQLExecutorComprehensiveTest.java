package minisql;

import org.junit.jupiter.api.*;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class SimpleSQLExecutorComprehensiveTest {

    @BeforeEach
    void clearAll() {
        // 清空所有表，保证独立测试环境
        TableManager.listTables().forEach(TableManager::dropTable);
    }

    @Test
    void testUnsupportedAndEmpty() {
        assertEquals("Unsupported SQL.", SimpleSQLExecutor.execute(""));
        assertEquals("Unsupported SQL.", SimpleSQLExecutor.execute("  "));
        assertEquals("Unsupported SQL.", SimpleSQLExecutor.execute("RANDOM COMMAND"));
    }

    @Test
    void testCreateTableSuccessAndDuplicate() {
        String res1 = SimpleSQLExecutor.execute(
                "CREATE TABLE users (id INT, name TEXT, PRIMARY KEY(id))");
        assertEquals("Table created: users", res1);

        // 再次创建同名表应提示已存在
        String res2 = SimpleSQLExecutor.execute(
                "CREATE TABLE users (id INT, name TEXT, PRIMARY KEY(id))");
        assertTrue(res2.contains("already exists"));
    }

    @Test
    void testCreateTableInvalidSyntax() {
        String res = SimpleSQLExecutor.execute("CREATE TABLE users id INT, name TEXT PRIMARY KEY(id)");
        assertEquals("Invalid CREATE syntax.", res);
    }

    @Test
    void testDropTableSuccessAndNotFound() {
        SimpleSQLExecutor.execute(
                "CREATE TABLE t (k INT, v TEXT, PRIMARY KEY(k))");

        String res1 = SimpleSQLExecutor.execute("DROP TABLE t");
        assertEquals("Table dropped: t", res1);

        String res2 = SimpleSQLExecutor.execute("DROP TABLE t");
        assertTrue(res2.contains("not found"));
    }

    @Test
    void testInsertAndSelectAllBasic() {
        SimpleSQLExecutor.execute(
                "CREATE TABLE items (id INT, name TEXT, PRIMARY KEY(id))");

        assertEquals("OK", SimpleSQLExecutor.execute(
                "INSERT INTO items (id, name) VALUES (1, 'A')"));
        assertEquals("OK", SimpleSQLExecutor.execute(
                "INSERT INTO items (id, name) VALUES (2, 'B')"));

        String all = SimpleSQLExecutor.execute("SELECT * FROM items");
        assertTrue(all.contains("{id=1, name=A}"));
        assertTrue(all.contains("{id=2, name=B}"));
    }

    @Test
    void testInsertColumnMismatch() {
        SimpleSQLExecutor.execute(
                "CREATE TABLE x (id INT, name TEXT, PRIMARY KEY(id))");

        // 列数不匹配
        String r1 = SimpleSQLExecutor.execute(
                "INSERT INTO x (id) VALUES (1, 'TooMany')");
        assertTrue(r1.contains("Column/value count mismatch"));

        // 未定义列
        String r2 = SimpleSQLExecutor.execute(
                "INSERT INTO x (id, age) VALUES (1, 30)");
        assertTrue(r2.contains("Column names mismatch"));
    }

    @Test
    void testInsertTypeMismatch() {
        SimpleSQLExecutor.execute(
                "CREATE TABLE x (id INT, name TEXT, PRIMARY KEY(id))");

        // id 为 INT 却插入字符串
        String r = SimpleSQLExecutor.execute(
                "INSERT INTO x (id, name) VALUES ('abc', 'Test')");
        assertTrue(r.contains("expects INT"));
    }

    @Test
    void testPrimaryKeyDuplicate() {
        SimpleSQLExecutor.execute(
                "CREATE TABLE x (id INT, name TEXT, PRIMARY KEY(id))");
        assertEquals("OK", SimpleSQLExecutor.execute(
                "INSERT INTO x (id, name) VALUES (1, 'One')"));

        String r2 = SimpleSQLExecutor.execute(
                "INSERT INTO x (id, name) VALUES (1, 'Dup')");
        assertTrue(r2.contains("Duplicate primary key"));
    }

    @Test
    void testSelectWherePKAndNonPK() {
        SimpleSQLExecutor.execute(
                "CREATE TABLE u (id INT, val TEXT, PRIMARY KEY(id))");
        SimpleSQLExecutor.execute(
                "INSERT INTO u (id, val) VALUES (10, 'Ten')");
        SimpleSQLExecutor.execute(
                "INSERT INTO u (id, val) VALUES (20, 'Twenty')");

        // 按主键查询
        String pkRes = SimpleSQLExecutor.execute(
                "SELECT * FROM u WHERE id = 20");
        assertTrue(pkRes.contains("val=Twenty"));

        // 按非主键列查询
        String nonPkRes = SimpleSQLExecutor.execute(
                "SELECT * FROM u WHERE val = 'Ten'");
        assertTrue(nonPkRes.contains("id=10"));

        // 查询不存在值
        String empty = SimpleSQLExecutor.execute(
                "SELECT * FROM u WHERE id = 999");
        assertEquals("Empty result.", empty);
    }

    @Test
    void testDeleteByPKAndByColAndClearTable() {
        SimpleSQLExecutor.execute(
                "CREATE TABLE u (id INT, val TEXT, PRIMARY KEY(id))");
        SimpleSQLExecutor.execute(
                "INSERT INTO u (id, val) VALUES (1, 'A')");
        SimpleSQLExecutor.execute(
                "INSERT INTO u (id, val) VALUES (2, 'A')");
        SimpleSQLExecutor.execute(
                "INSERT INTO u (id, val) VALUES (3, 'B')");

        // DELETE WHERE non-pk
        String d1 = SimpleSQLExecutor.execute(
                "DELETE FROM u WHERE val = 'A'");
        assertTrue(d1.contains("Deleted rows: 2"));

        // DELETE WHERE pk
        String d2 = SimpleSQLExecutor.execute(
                "DELETE FROM u WHERE id = 3");
        assertEquals("Deleted rows: 1", d2);

        // DELETE entire table
        String clr = SimpleSQLExecutor.execute("DELETE FROM u");
        assertTrue(clr.contains("Table cleared"));

        // SELECT after clear
        String sel = SimpleSQLExecutor.execute("SELECT * FROM u");
        assertEquals("Empty table.", sel);
    }

    @Test
    void testUpdateSuccessAndErrors() {
        SimpleSQLExecutor.execute(
                "CREATE TABLE p (id INT, name TEXT, PRIMARY KEY(id))");
        SimpleSQLExecutor.execute(
                "INSERT INTO p (id, name) VALUES (5, 'Five')");
        SimpleSQLExecutor.execute(
                "INSERT INTO p (id, name) VALUES (6, 'Six')");

        // 更新主键以外列
        String up1 = SimpleSQLExecutor.execute(
                "UPDATE p SET name = 'FIVE' WHERE id = 5");
        assertEquals("Updated rows: 1", up1);
        assertTrue(SimpleSQLExecutor.execute(
                "SELECT * FROM p WHERE id = 5").contains("FIVE"));

        // 更新不存在列
        String upErr = SimpleSQLExecutor.execute(
                "UPDATE p SET age = 30 WHERE id = 6");
        assertTrue(upErr.contains("Update error"));

        // 更新类型错误
        String upErr2 = SimpleSQLExecutor.execute(
                "UPDATE p SET id = 'bad' WHERE id = 6");
        assertTrue(upErr2.contains("Update error"));
    }

    @Test
    void testDropAndRecreate() {
        SimpleSQLExecutor.execute(
                "CREATE TABLE z (id INT, name TEXT, PRIMARY KEY(id))");
        SimpleSQLExecutor.execute(
                "INSERT INTO z (id, name) VALUES (1, 'X')");
        assertEquals("Table dropped: z", SimpleSQLExecutor.execute("DROP TABLE z"));

        // 再次 INSERT 会提示表不存在
        String res = SimpleSQLExecutor.execute(
                "INSERT INTO z (id, name) VALUES (1, 'X')");
        assertTrue(res.contains("Table not found"));

        // 重新 CREATE
        assertEquals("Table created: z", SimpleSQLExecutor.execute(
                "CREATE TABLE z (id INT, name TEXT, PRIMARY KEY(id))"));
    }
}
