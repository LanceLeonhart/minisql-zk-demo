package minisql;

import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

class SimpleSQLExecutorTest {

    @BeforeEach
    void clearTables() {
        // 每个测试前清空所有表
        TableManager.listTables().forEach(TableManager::dropTable);
    }

    @Test
    void testCreateAndDropTable() {
        String res1 = SimpleSQLExecutor.execute(
                "CREATE TABLE users (id INT, name TEXT, PRIMARY KEY(id))");
        assertEquals("Table created: users", res1);

        String res2 = SimpleSQLExecutor.execute(
                "CREATE TABLE users (id INT, name TEXT, PRIMARY KEY(id))");
        assertTrue(res2.contains("already exists"));

        String res3 = SimpleSQLExecutor.execute("DROP TABLE users");
        assertEquals("Table dropped: users", res3);

        String res4 = SimpleSQLExecutor.execute("DROP TABLE users");
        assertTrue(res4.contains("not found"));
    }

    @Test
    void testInsertAndSelectAll() {
        SimpleSQLExecutor.execute(
                "CREATE TABLE items (id INT, name TEXT, PRIMARY KEY(id))");

        String r1 = SimpleSQLExecutor.execute(
                "INSERT INTO items (id, name) VALUES (1, 'A')");
        assertEquals("OK", r1);

        String r2 = SimpleSQLExecutor.execute(
                "INSERT INTO items (id, name) VALUES (2, 'B')");
        assertEquals("OK", r2);

        String sel = SimpleSQLExecutor.execute("SELECT * FROM items");
        assertTrue(sel.contains("id=1"));
        assertTrue(sel.contains("id=2"));
    }

    @Test
    void testSelectWhereAndDelete() {
        SimpleSQLExecutor.execute(
                "CREATE TABLE t (k INT, v TEXT, PRIMARY KEY(k))");
        SimpleSQLExecutor.execute("INSERT INTO t (k, v) VALUES (1, 'X')");
        SimpleSQLExecutor.execute("INSERT INTO t (k, v) VALUES (2, 'Y')");

        String sel1 = SimpleSQLExecutor.execute("SELECT * FROM t WHERE k = 2");
        assertTrue(sel1.contains("v=Y"));

        String del1 = SimpleSQLExecutor.execute("DELETE FROM t WHERE k = 2");
        assertEquals("Deleted rows: 1", del1);

        String sel2 = SimpleSQLExecutor.execute("SELECT * FROM t");
        assertFalse(sel2.contains("v=Y"));
    }

    @Test
    void testUpdate() {
        SimpleSQLExecutor.execute(
                "CREATE TABLE u (id INT, name TEXT, PRIMARY KEY(id))");
        SimpleSQLExecutor.execute("INSERT INTO u (id, name) VALUES (10, 'Foo')");

        String upd = SimpleSQLExecutor.execute(
                "UPDATE u SET name='Bar' WHERE id = 10");
        assertEquals("Updated rows: 1", upd);

        String sel = SimpleSQLExecutor.execute("SELECT * FROM u WHERE id = 10");
        assertTrue(sel.contains("Bar"));
    }
}
