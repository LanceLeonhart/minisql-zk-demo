package minisql;

/**
 * 表的列定义：名称 + 类型
 */
public class Column {
    private final String name;
    private final String type; // "INT" 或 "TEXT"

    public Column(String name, String type) {
        this.name = name;
        this.type = type.toUpperCase();
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }
}
