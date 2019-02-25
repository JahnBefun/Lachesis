package com.chosenboy.util.sql.analyzer;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLUseStatement;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.druid.stat.TableStat;
import com.alibaba.druid.util.JdbcConstants;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

public class DruidAnalyzer {
    public static Map<String, TreeSet<String>> getFromTo (String sql) throws ParserException {
        List<SQLStatement> stmts = SQLUtils.parseStatements(sql, JdbcConstants.HIVE);
        TreeSet<String> fromSet = new TreeSet<>();
        TreeSet<String> toSet = new TreeSet<>();
        if (stmts == null) {
            return null;
        }

        String database="DEFAULT";
        for (SQLStatement stmt : stmts) {
            SchemaStatVisitor statVisitor = SQLUtils.createSchemaStatVisitor(JdbcConstants.HIVE);
            if (stmt instanceof SQLUseStatement) {
                database = ((SQLUseStatement) stmt).getDatabase().getSimpleName().toUpperCase();
            }
            stmt.accept(statVisitor);
            Map<TableStat.Name, TableStat> tables = statVisitor.getTables();
            if (tables != null) {
                final String db = database;
                tables.forEach((tableName, stat) -> {
                    if (stat.getCreateCount() > 0 || stat.getInsertCount() > 0) {
                        String to = tableName.getName().toUpperCase();
                        if (!to.contains("."))
                            to = db + "." + to;
                        toSet.add(to);
                    } else if (stat.getSelectCount() > 0) {
                        String from = tableName.getName().toUpperCase();
                        if (!from.contains("."))
                            from = db + "." + from;
                        fromSet.add(from);
                    }
                });
            }
        }

        Map<String, TreeSet<String>> fromTo = new HashMap<>(4);
        fromTo.put("from", fromSet);
        fromTo.put("to", toSet);
        return fromTo;
    }

    public static void main(String[] args) {
       String sql = "-- step 1 step1\n" +
                "USE db1;\n" +
                "DROP TABLE IF EXISTS tb1;\n" +
                "CREATE TABLE db1.tb1 STORED AS PARQUET AS\n" +
                "SELECT filed1,\n" +
                "       filed2,\n" +
                "       filed3\n" +
                "FROM db2.tb2\n" +
                "WHERE filed1 = 0\n" +
                "  AND filed3 = 0\n" +
                "UNION ALL\n" +
                "SELECT a.filed4 AS filed4,\n" +
                "       a.filed5\n" +
                "FROM db1.tb2 a\n" +
                "LEFT JOIN db2.tb3 b ON (a.filed4 = b.filed4)\n" +
                "WHERE a.filed4 = 1\n" +
                "  AND a.filed5 = 0;\n";
        Map<String, TreeSet<String>> result = DruidAnalyzer.getFromTo(sql);
        result.forEach((key, set) -> {
            System.out.println(key);
            set.forEach(item -> System.out.println(item));
        });
    }
}
