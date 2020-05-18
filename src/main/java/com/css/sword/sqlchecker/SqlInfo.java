package com.css.sword.sqlchecker;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SqlInfo {
    public final File file;
    public final String key;
    public final String sql;
    public final int hashCode;
    boolean multiTable = false;
    final Map<String, String> tablesAndFpj = new HashMap<String, String>();
    final Map<String, String> alias = new HashMap<String, String>();
    final Set<String> owners = new HashSet<String>();
    boolean isUnionAll = false;
    List<String> fpTables = new ArrayList<String>(2);

    SqlInfo(File file, String key, String sql) {
        this.file = file;
        this.key = key;
        this.sql = processMacroParameter(sql);
        this.hashCode = this.sql.hashCode();
    }

    public SqlInfo(String key, String sql) {
        this(null, key, sql);
    }

    private String processMacroParameter(String sql) {
        //replaceAll : 使用给定的参数 replacement 替换字符串所有匹配给定的正则表达式的子字符串。
        sql = sql.replaceAll("#macroparam#", "?");
        sql = sql.replaceAll("#if\\(\\S+\\)#", "");
        sql = sql.replaceAll("#endif#", "");
        sql = sql.replaceAll("\\$\\S+\\$", "?");
        return sql;
    }

    @Override
    public String toString() {
        return "{" + key + ":" + hashCode + '}';
    }
}