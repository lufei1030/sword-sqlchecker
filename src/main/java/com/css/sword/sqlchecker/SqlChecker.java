package com.css.sword.sqlchecker;

import java.io.Closeable;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.expr.SQLBetweenExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLCastExpr;
import com.alibaba.druid.sql.ast.expr.SQLExistsExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLInListExpr;
import com.alibaba.druid.sql.ast.expr.SQLInSubQueryExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.ast.expr.SQLValuableExpr;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLSubqueryTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.ast.statement.SQLUnionQuery;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.SQLParserUtils;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.css.sword.kernel.base.persistence.database.handler.ISwordDataSource;
import com.css.sword.kernel.platform.SwordComponentRef;
import until.LinkedMultiValueMap;


public class SqlChecker implements Closeable {

    private Connection connection;
    private final Properties wlist = new Properties();
    private Map<String, String[]> tabInfos = new HashMap<String, String[]>();
    private Map<String, TreeMap<String, AtomicInteger>> cInfo = new HashMap<String, TreeMap<String, AtomicInteger>>();
    public StringBuilder errorMessage = new StringBuilder();
    public StringBuilder dbaErrorMessage = new StringBuilder();
    public StringBuilder columnRefInfo = new StringBuilder();
    private int errCount = 0;

    private static LinkedMultiValueMap<String,Object> errorData=new LinkedMultiValueMap<String, Object>();
    private static String SJMX_TABLE = "HX_SJMX.SJMX_TABLE";

    public static void main(String[] args) throws Exception {
        String url = "jdbc:oracle:thin:@10.23.11.34:1521:csstax";
        String user = "HX_USER";
        String password = "css";

        String whiteFile = null;
        String sqlXmlFileDir = null;

       if (args.length < 2) {
            System.err.println("请配置白名单文件绝对路径，及要扫描的sql所在的目录");
            return;
        }

        if (args.length >= 5) {
            url = args[0];
            user = args[1];
            password = args[2];
            whiteFile = args[3];
            sqlXmlFileDir = args[4];
            if (args.length > 5) {
                SJMX_TABLE = args[5];
            }
        } else if (args.length >= 2) {
            whiteFile = args[0];
            sqlXmlFileDir = args[1];
            if (args.length > 2) {
                SJMX_TABLE = args[2];
            }
        } else {
            System.err.println("参数个数不正确");
            return;
        }

        final SqlChecker checker = new SqlChecker(url, user, password, whiteFile);
        checker.checkFiles(sqlXmlFileDir);
        checker.printColumnRanker();
        checker.insterBatchList(errorData);
        checker.close();
    }

    public SqlChecker(String url, String user, String password, String wListFile) throws Exception {
        Class.forName("oracle.jdbc.OracleDriver");
        init(DriverManager.getConnection(url, user, password), wListFile);
    }

    public SqlChecker(String datasource, String wListFile) throws Exception {
        init(this.getSwordConnection(datasource), wListFile);
    }

    private Connection getSwordConnection(String dsName) {
        try {// 主要是为了兼容工具老版本的核心框架代码
            ISwordDataSource ds = SwordComponentRef.persistenceManager.getDsManager().getDsByName(dsName);
            if (ds == null) {
                throw new SQLException("不存在名为" + dsName + "的数据源");
            }
            return ds.getDataSource().getConnection();
        } catch (Exception e) {
            throw new RuntimeException("获取sword连接时出错", e);
        }
    }

    private void init(Connection connection, String wListFile) throws Exception {
        System.out.println("成功初始化数据库连接");
        this.connection = connection;
        System.out.println("加载白名单文件:" + wListFile);
        wlist.load(new FileReader(wListFile));
    }

    public void checkFiles(String path) throws Exception {
        System.out.println("开始执行sql扫描");
        for (final File file : new File(path).listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".xml");
            }
        }))
        {
            System.out.println("开始解析文件" + file);
            String xml_name=file.getName();
            final SAXReader reader = new SAXReader();
            final Document doc = reader.read(file);
            try {
                for (final Object element : doc.getRootElement().elements("sql")) {
                    final Element el = (Element) element;
                    final String key = el.attributeValue("key");
                    final String sql = el.attributeValue("sql");
                    final String tables = el.attributeValue("tables");
                    errorData.add("xml_name",xml_name);
                    errorData.add("sql-key",key);
                    errorData.add("sql_dml",sql);
                    errorData.add("表名",tables);
                    //sqlinfo 返回hashCode值
                    final SqlInfo sqlInfo = new SqlInfo(file, key, sql);
                    try {
                        checkSQL(sqlInfo);
                    } catch (ParserException ex) {
                        System.err.println("SQL语法错误，请检查:" + sqlInfo);
                        errorData.add("错误信息",("SQL语法错误，请检查:" + sqlInfo));
                        errCount++;
                    } catch (Exception ex) {
                        dbaErrorMessage.append(sqlInfo).append("\n");
                    }
                    if(errCount == 0){
                        errorData.add("错误信息",key+":此SqlKey对应SQL语句效对无误");
                    }
                }
            } finally {
                if (dbaErrorMessage.length() > 0) {
                    System.err.println("SQL解析失败，请DBA人工审核:\n" + dbaErrorMessage);
                    errorData.add("错误信息",("SQL解析失败，请DBA人工审核:\n" + dbaErrorMessage));
                }
                if (columnRefInfo.length() > 0) {
                    System.out.println("表及过滤条件字段的引用次数信息：\n" + columnRefInfo);
                }
            }
        }
    }

    // 配置管理工具也会调用此接口
    public void checkSQL(final SqlInfo sqlInfo) throws Exception {
        this.errorMessage = null;
        this.errCount = 0;
        try {
            //sql白名单检查
            if (checkWList(sqlInfo)) return;
            // 从配置管理工具查询，只有DDM类型的sql才检查
            if (!this.isDdmTypeSql(sqlInfo.key)) {
                return;
            }

            final SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sqlInfo.sql, "mysql");
            final SQLStatement sqlStatement = parser.parseStatementList().get(0);
            if (!(sqlStatement instanceof SQLSelectStatement)) {
                return;
            }

            SQLSelectQuery query = (((SQLSelectStatement) sqlStatement).getSelect()).getQuery();

            // select 1/*/x from dual
            if (isSelectDual(query)) return;

            // 检查出现union的次数，只能用一个union
            if (!this.checkUnionAllCount(sqlInfo)) {
                return;
            }

            checkSQLQuery(sqlInfo, query);

            // 检查union中不允许出现分片键
            if (sqlInfo.isUnionAll && sqlInfo.fpTables.size() > 0) {
                printError("union中存在分片表: " + sqlInfo.fpTables, sqlInfo);
            }
        } finally {
            if (this.errorMessage != null && this.errorMessage.length() > 0) {
                System.err.println(sqlInfo + "语句包含如下错误，请及时修正：");
                System.err.println(this.errorMessage);
                errorData.add("错误信息",this.errorMessage);
            }
        }
    }
    private boolean checkUnionAllCount(SqlInfo sqlInfo) {
        String sql = sqlInfo.sql.toLowerCase();
        if (sql.indexOf("union", sql.indexOf("union") + 1) > 0) {
            printError("只能使用一个union", sqlInfo);
            return false;
        }
        return true;
    }
    // select 1/*/x from dual
    private boolean isSelectDual(SQLSelectQuery query) {
        if (query instanceof SQLSelectQueryBlock) {
            SQLTableSource from = ((SQLSelectQueryBlock) query).getFrom();
            if (from instanceof SQLExprTableSource) {
                if (((SQLExprTableSource) from).getName().getSimpleName().equals("dual")) {
                    return true;
                }
            }
        }
        return false;
    }
    private void checkSQLQuery(final SqlInfo sqlInfo, final SQLSelectQuery query) throws Exception {
        if (query instanceof SQLSelectQueryBlock) {
            final SQLSelectQueryBlock select = (SQLSelectQueryBlock) query;
            checkSQLQueryBlock(select, sqlInfo);
        } else if (query instanceof SQLUnionQuery) {
            sqlInfo.isUnionAll = true;
            SQLUnionQuery uquery = (SQLUnionQuery) query;
            {
                SQLSelectQuery left = uquery.getLeft();
                SqlInfo subSqlInfo = new SqlInfo(sqlInfo.file, sqlInfo.key, left.toString());
                checkSQLQuery(subSqlInfo, left);
                if (subSqlInfo.fpTables.size() > 0) {
                    sqlInfo.fpTables.addAll(subSqlInfo.fpTables);
                }
            }
            {
                SQLSelectQuery right = uquery.getRight();
                SqlInfo subSqlInfo = new SqlInfo(sqlInfo.file, sqlInfo.key, right.toString());
                checkSQLQuery(subSqlInfo, right);
                if (subSqlInfo.fpTables.size() > 0) {
                    sqlInfo.fpTables.addAll(subSqlInfo.fpTables);
                }
            }
        }
    }

    private void checkSQLQueryBlock(SQLSelectQueryBlock select, SqlInfo sqlInfo) throws Exception {
        //子查询和exists语法检查
        checkStatement(sqlInfo, select);
        //解析from
        parseFrom(sqlInfo, select.getFrom());
        //检查目标表是否是同一个数据库
        checkTableOwner(sqlInfo);
        //解析where
        parseExpr(sqlInfo, select.getWhere(), false, false);
        //分片键缺失或值不等问题的扫描
        checkFpj(sqlInfo);
    }

    private boolean checkWList(SqlInfo sqlInfo) throws Exception {
        final Object hashCode = wlist.get(sqlInfo.key);
        return hashCode != null && hashCode.equals(Integer.toString(sqlInfo.sql.hashCode()));
    }

    private void checkStatement(final SqlInfo sqlInfo, SQLSelectQueryBlock select) throws Exception {
        final String sql = sqlInfo.sql.toLowerCase();

        boolean hasSubQuery = false;
        if (sql.indexOf("select", sql.indexOf("where") + 6) > 0) {
            hasSubQuery = true;
            printError("禁止使用子查询", sqlInfo);
        }
        if (sql.contains("exists")) {
            printError("禁止使用exists或not exists", sqlInfo);
        }
        if (sql.contains("limit") && !sql.contains("order")) {
            boolean error = true;
            if (!hasSubQuery) {
                SQLTableSource from = select.getFrom();
                if (from instanceof SQLExprTableSource) {
                    final SQLExprTableSource tableSource = (SQLExprTableSource) from;
                    final SQLExpr expr = tableSource.getExpr();
                    String tableName;
                    if (expr instanceof SQLPropertyExpr) {
                        tableName = ((SQLPropertyExpr) expr).getName();
                    } else {
                        tableName = expr.toString();
                    }
                    String[] fpxx = this.queryFpxx(tableName, sqlInfo);
                    if (fpxx == null) {//单表允许使用
                        error = false;
                    }
                }
            }
            if (error) {
                printError("使用limit子句时必须同时增加order by子句", sqlInfo);
            }
        }

        if (sql.contains("row_number")) {
            printError("禁止使用row_number函数", sqlInfo);
        } else if (sql.contains("rank")) {
            printError("禁止使用rank函数", sqlInfo);
        } else if (sql.contains("dense_rank")) {
            printError("禁止使用dense_rank函数", sqlInfo);
        } else if (sql.contains("first_value")) {
            printError("禁止使用first_value函数", sqlInfo);
        } else if (sql.contains("partition")) {
            printError("禁止使用partition关键字", sqlInfo);
        } else if (sql.contains("sys_guid")) {
            printError("禁止使用sys_guid函数", sqlInfo);
        } else if (sql.contains("decode")) {
            printError("禁止使用decode，请使用case语句代替", sqlInfo);
        }

    }

    private final String joinTableAlias = "$$joainTableAlias$$";

    private void parseFrom(final SqlInfo sqlInfo, final SQLTableSource from) throws Exception {
        if (from instanceof SQLJoinTableSource) {
            sqlInfo.multiTable |= true;
            final SQLJoinTableSource join = (SQLJoinTableSource) from;
            parseFrom(sqlInfo, join.getLeft());
            parseFrom(sqlInfo, join.getRight());
        } else if (from instanceof SQLExprTableSource) {
            final SQLExprTableSource tableSource = (SQLExprTableSource) from;
            final SQLExpr expr = tableSource.getExpr();
            String owner = null;
            String tableName;
            if (expr instanceof SQLPropertyExpr) {
                owner = ((SQLPropertyExpr) expr).getOwnernName();
                tableName = ((SQLPropertyExpr) expr).getName();
            } else {
                tableName = expr.toString();
            }
            queryFpxx(tableName, sqlInfo);
            sqlInfo.owners.add(owner);
            tableName = tableName.toLowerCase();
            String alias = tableSource.getAlias();
            TreeMap<String, AtomicInteger> map = cInfo.get(tableName);
            if (map == null) {
                map = new TreeMap<String, AtomicInteger>();
                map.put("*", new AtomicInteger(0));
                cInfo.put(tableName, map);
            }
            map.get("*").incrementAndGet();//表引用次数计数器
            sqlInfo.tablesAndFpj.put(tableName, null);
            if (alias != null) {
                alias = alias.toLowerCase();
                sqlInfo.alias.put(alias, tableName);
            }
        } else if (from instanceof SQLSubqueryTableSource) {
//            sqlInfo.tablesAndFpj.put(from.getAlias(), null);//子查询表别名不查询分片信息
            String alias = from.getAlias();
            if (alias != null) {
                alias = alias.toLowerCase();
                sqlInfo.alias.put(alias, joinTableAlias);
            }
        } else {
            printError("不支持的查询类型:" + from, sqlInfo);
        }
    }

    private void checkTableOwner(SqlInfo sqlInfo) {
        if (sqlInfo.owners.size() > 1) {
            printError("目标表不在同一个数据库" + sqlInfo.owners + "，不能进行关联查询", sqlInfo);
        }
    }

    private String[] parseExpr(final SqlInfo sqlInfo, final SQLExpr expr, final boolean inFunction, final boolean in) throws Exception {
        String[] v = null;
        if (expr == null) {
            printError("谨慎使用无任何过滤条件的sql", sqlInfo);
        } else if (expr instanceof SQLBinaryOpExpr) {
            parseBinaryOpExpr(sqlInfo, (SQLBinaryOpExpr) expr, inFunction, in);
        } else if (expr instanceof SQLPropertyExpr) {
            v = parsePropertyExpr(sqlInfo, (SQLPropertyExpr) expr, inFunction);
        } else if (expr instanceof SQLIdentifierExpr) {
            v = parseIdentifierExpr(sqlInfo, (SQLIdentifierExpr) expr, inFunction);
        } else if (expr instanceof SQLValuableExpr) {
            //常量值
            parseVarOrValuExpr(sqlInfo, expr, inFunction, in);
        } else if (expr instanceof SQLVariantRefExpr) {
            //变量值
            parseVarOrValuExpr(sqlInfo, expr, inFunction, in);
        } else if (expr instanceof SQLInListExpr) {
            parseInListExpr((SQLInListExpr) expr, sqlInfo);
        } else if (expr instanceof SQLMethodInvokeExpr || expr instanceof SQLAggregateExpr) {
            parseMethodInvoke(expr, sqlInfo, in);
        } else if (expr instanceof SQLBetweenExpr) {
            parseBetweenExpr(sqlInfo, (SQLBetweenExpr) expr, inFunction, in);
        } else if (expr instanceof SQLQueryExpr || expr instanceof SQLInSubQueryExpr || expr instanceof SQLExistsExpr) {
        } else if (expr instanceof SQLCastExpr) {
            final SQLExpr subExpr = ((SQLCastExpr) expr).getExpr();
            v = parseExpr(sqlInfo, subExpr, inFunction, in);
        } else {
            printError("禁止使用的表达式语法:" + expr, sqlInfo);
        }
        return v;
    }

    private void parseBinaryOpExpr(final SqlInfo sqlInfo, final SQLBinaryOpExpr expr, final boolean inFunction, final boolean in) throws Exception {
        SQLExpr left = expr.getLeft();
        SQLExpr right = expr.getRight();
        final String[] leftOwner = parseExpr(sqlInfo, left, inFunction, in);
        final String[] rightOwner = parseExpr(sqlInfo, right, inFunction, in);
        if (leftOwner != null ^ rightOwner != null) {
            String[] owner = leftOwner;
            SQLExpr subExpr = right;
            if (rightOwner != null) {
                subExpr = left;
                left = right;
                right = subExpr;
                owner = rightOwner;
            }
            final String operator = expr.getOperator().name;
            if (operator.equalsIgnoreCase("like")) {
                printError("禁止分片键" + left + "使用like", sqlInfo);
            } else if (owner[1].equals("1")) {
                if (!"=".equals(operator)) {
                    printError("禁止Hash算法分片键" + left + "使用>、>=、<、<=、!=等比较运算符", sqlInfo);
                }
            }
            if (subExpr instanceof SQLVariantRefExpr) {
                sqlInfo.tablesAndFpj.put(owner[0], "?");
            } else if (subExpr instanceof SQLValuableExpr) {
                sqlInfo.tablesAndFpj.put(owner[0], ((SQLValuableExpr) subExpr).getValue().toString());
            } else {
                printError("分片键" + left + "条件表达式的另一侧不是分片键、参数或常量:" + expr, sqlInfo);
            }
        }
    }

    private void parseBetweenExpr(SqlInfo sqlInfo, SQLBetweenExpr expr, boolean inFunction, boolean in) throws Exception {
        final SQLExpr testExpr = expr.getTestExpr();
        String[] owner = parseExpr(sqlInfo, testExpr, inFunction, in);
        if (owner != null && "1".equals(owner[1])) {
            printError("禁止Hash算法分片键" + testExpr + "使用between运算符", sqlInfo);
        }
        parseExpr(sqlInfo, expr.getBeginExpr(), inFunction, in);
        parseExpr(sqlInfo, expr.getEndExpr(), inFunction, in);
    }

    private String[] parsePropertyExpr(final SqlInfo sqlInfo, final SQLPropertyExpr expr, final boolean inFunction) throws Exception {
        final String owner = expr.getOwnernName();
        final String name = expr.getName();
        return parseColumn(sqlInfo, owner, name, expr.toString(), inFunction);
    }

    private String[] parseIdentifierExpr(final SqlInfo sqlInfo, final SQLIdentifierExpr expr, final boolean inFunction) throws Exception {
        final String name = expr.getName();
        if (name.equalsIgnoreCase("sysdate")) {
            printError("禁止使用" + name, sqlInfo);
            return null;
        }
        if (sqlInfo.tablesAndFpj.size() > 1) {
            printError("字段" + name + "未明确指定所属表", sqlInfo);
            return null;
        }
        final String owner = sqlInfo.tablesAndFpj.keySet().iterator().next();
        if (owner == null) {
            return null;
        } else {
            return parseColumn(sqlInfo, owner, name, expr.toString(), inFunction);
        }
    }

    private String[] parseColumn(final SqlInfo sqlInfo, String owner, String name, final String expr, final boolean inFunction) throws Exception {
        owner = owner.toLowerCase();
        if (sqlInfo.alias.containsKey(owner)) {
            owner = sqlInfo.alias.get(owner);
        }
        name = name.toLowerCase();

        if (owner.equals(joinTableAlias)) {
            return null;
        }

        final String[] fpxx = queryFpxx(owner, sqlInfo);
        if (fpxx == null) {
            return null;
        }
        final boolean isFpj = fpxx[1] != null && fpxx[1].equalsIgnoreCase(name);
        if (inFunction && isFpj) {
            printError("禁止将分片键" + expr + "作为函数参数", sqlInfo);
        }
        if (!inFunction) {
            final AtomicInteger count = cInfo.get(owner).get(name);
            if (count == null) {
                cInfo.get(owner).put(name, new AtomicInteger(1));
            } else {
                count.incrementAndGet();
            }
        }
        return isFpj ? new String[]{owner, fpxx[2]} : null;
    }

    private void parseInListExpr(final SQLInListExpr expr, final SqlInfo sqlInfo) throws Exception {
        final String[] owner = parseExpr(sqlInfo, expr.getExpr(), false, true);
        final List<SQLExpr> targetList = expr.getTargetList();
        for (int i = 0; i < targetList.size(); i++) {
            parseExpr(sqlInfo, targetList.get(i), false, true);
        }
        if (owner != null) {
            if (expr.isNot()) {
                printError("禁止分片键" + expr.getExpr() + "使用not in", sqlInfo);

            }
            sqlInfo.tablesAndFpj.put(owner[0], targetList.toString());
        }
    }

    private void parseMethodInvoke(final SQLExpr expr, final SqlInfo sqlInfo, final boolean in) throws Exception {
        final List<SQLExpr> exprs = expr instanceof SQLMethodInvokeExpr ? ((SQLMethodInvokeExpr) expr).getArguments() : ((SQLAggregateExpr) expr).getArguments();
        if (exprs.isEmpty()) {
            return;
        }
        for (final SQLExpr subExpr : exprs) {
            parseExpr(sqlInfo, subExpr, true, in);
        }
    }

    private void parseVarOrValuExpr(final SqlInfo sqlInfo, final SQLExpr expr, final boolean inFunction, final boolean in) {
        if (inFunction && in) {
            printError("禁止在in表达式中使用函数对参数或常量[" + expr + "]进行计算", sqlInfo);
        }
    }

    private void checkFpj(final SqlInfo sqlInfo) throws Exception {
        String v = null;
        final List<String> db = new ArrayList<String>();
        final List<String> fpb = new ArrayList<String>();
        for (Map.Entry<String, String> entry : sqlInfo.tablesAndFpj.entrySet()) {
            final String table = entry.getKey();
            final String fpj = entry.getValue();
            //跳过单表
            if (tabInfos.get(table) == null) {
                printError("表" + table + "的分片定义信息不存在", sqlInfo);
            } else if (tabInfos.get(table)[1] == null) {
                db.add(table);
            } else {
                fpb.add(table);
                //获取第一个库名
                if (v == null) {
                    v = fpj;
                }
                //检查是否存在分片键条件
                else if (fpj == null) {
                    printError("缺少分片表" + table + "的分片键过滤条件", sqlInfo);
                }
                //检查是否存在分片键不等
                else if (v != null && !v.equals(fpj)) {
                    printError("各分片表的分片键过滤条件存在不相等问题", sqlInfo);
                }
            }
        }
        if (!db.isEmpty() && !fpb.isEmpty()) {
            printError("禁止单表" + db + "与分片表" + fpb + "关联操作", sqlInfo);
        }
    }

    private String[] queryFpxx(String tableName, SqlInfo sqlInfo) throws Exception {
        tableName = tableName.toLowerCase();
        if (tabInfos.containsKey(tableName)) {
            return tabInfos.get(tableName);
        }
        PreparedStatement ps = null;
        ResultSet resultSet = null;

        try {
            ps = this.connection.prepareStatement("SELECT owner,cfj,fpgz_dm FROM " + SJMX_TABLE + " t where t.table_sjmxname = ?");
            ps.setString(1, tableName.toUpperCase());

            resultSet = ps.executeQuery();
            if (!resultSet.next()) {
                printError("未找到表" + tableName + "的分片键定义信息", sqlInfo);
                return null;
            } else {
                final String[] fpxx = new String[3];
                fpxx[0] = resultSet.getString(1);
                fpxx[1] = resultSet.getString(2);
                fpxx[2] = resultSet.getString(3);
                if (fpxx[1] != null && fpxx[1].trim().equals("")) {
                    fpxx[1] = null;
                }
                tabInfos.put(tableName, fpxx);

                if (fpxx[1] != null) {
                    sqlInfo.fpTables.add(tableName);
                }
                return fpxx;
            }
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
            if (ps != null) {
                ps.close();
            }
        }
    }

    private void printError(final String message, final SqlInfo sqlInfo) {
        if (this.errorMessage == null) {
            this.errorMessage = new StringBuilder();
        }
        this.errorMessage.append(++errCount).append(".").append(message).append("\n");
    }

    private void printColumnRanker() {
        System.out.println("\n\n-----------表引用次数及条件列引用次数---------------------------------------");
        for (Map.Entry<String, TreeMap<String, AtomicInteger>> entry : cInfo.entrySet()) {
            final TreeMap<String, AtomicInteger> map = entry.getValue();
            final int tableRefCount = map.remove("*").get();
            System.out.println("-----------" + entry.getKey() + ":" + tableRefCount + "--------------------------------------");

            final List<Map.Entry<String, AtomicInteger>> list = new ArrayList<Map.Entry<String, AtomicInteger>>(map.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<String, AtomicInteger>>() {
                @Override
                public int compare(Map.Entry<String, AtomicInteger> o1, Map.Entry<String, AtomicInteger> o2) {
                    return o2.getValue().get() - o1.getValue().get();
                }
            });
            if(list.size()==0){
                errorData.add("引用表名",entry.getKey());
                errorData.add("表引用次数",tableRefCount);
                errorData.add("引用字段","此表暂无引用字段");
                errorData.add("字段引用次数","此表暂无引用字段");
            }
            for (Map.Entry<String, AtomicInteger>   subEntry : list) {
                System.out.println("\t" + subEntry.getKey() + " ---> " + subEntry.getValue());
                errorData.add("引用表名",entry.getKey());
                errorData.add("表引用次数",tableRefCount);
                errorData.add("引用字段",subEntry.getKey());
                errorData.add("字段引用次数",subEntry.getValue());
            }


        }
    }

    // 查询sqlKey在配置管理工具配置的类型是否是DDM
    private boolean isDdmTypeSql(String sqlKey) throws SQLException {
        PreparedStatement ps = null;
        ResultSet resultSet = null;

        try {
            ps = this.connection.prepareStatement("select sql_type from pzgl_sql t where t.sql_key = ? and yx_bj='Y' and rownum = 1 order by jc_sql_version desc");
            ps.setString(1, sqlKey);

            resultSet = ps.executeQuery();
            if (resultSet.next()) {
                String sqlType = resultSet.getString(1);
                // ddm=01
                if ("01".equals(sqlType)) {
                    return true;
                }
                return false;
            } else {// 没有配置的，默认也认为需要检查
                return true;
            }
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
            if (ps != null) {
                ps.close();
            }
        }
    }

    private void insterBatchList(LinkedMultiValueMap<String,Object> data) throws SQLException {

        PreparedStatement insterPs = null;
        PreparedStatement columnInsterPs = null;
        ResultSet insterSet = null;

        try {
            insterPs = this.connection.prepareStatement("INSERT INTO ERRORTABLE_RECORD VALUES(?,?,?,?,?,?,?)");
                int index=0;
                List<Object> values=data.getValues("sql-key");
                for(Object value: values){
                    UUID uuid = UUID.randomUUID();
                    java.util.Date time = new java.util.Date();
                    java.sql.Timestamp ts = new java.sql.Timestamp(time.getTime());
                    insterPs.setString(1,uuid.toString());
                    insterPs.setString(2,(data.getValue("xml_name", index)).toString());
                    insterPs.setString(3,(data.getValue("sql-key", index)).toString());
                    insterPs.setString(4,(data.getValue("表名", index)).toString());
                    insterPs.setString(5,(data.getValue("sql_dml", index)).toString());
                    insterPs.setString(6,(data.getValue("错误信息", index)).toString());
                    insterPs.setTimestamp(7,ts);
                    index++;
                    insterPs.executeQuery();
                }
            columnInsterPs = this.connection.prepareStatement("INSERT INTO ERRORTABLE_COLUMN_COUNT VALUES(?,?,?,?,?,?)");
            int columnIndex=0;
                List<Object> columnValues = data.getValues("引用表名");
                for(Object value: columnValues){
                    UUID columnUuid = UUID.randomUUID();
                    java.util.Date time = new java.util.Date();
                    java.sql.Timestamp ts = new java.sql.Timestamp(time.getTime());
                    columnInsterPs.setString(1,columnUuid.toString());
                    columnInsterPs.setString(2,(data.getValue("引用表名", columnIndex)).toString());
                    columnInsterPs.setString(3,(data.getValue("表引用次数", columnIndex)).toString());
                    columnInsterPs.setString(4,(data.getValue("引用字段", columnIndex)).toString());
                    columnInsterPs.setString(5,(data.getValue("字段引用次数", columnIndex)).toString());
                    columnInsterPs.setTimestamp(6,ts);
                    columnIndex++;
                    insterSet=columnInsterPs.executeQuery();
                }

        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            if (insterPs != null) {
                insterPs.close();
            }
            if(columnInsterPs !=null){
                columnInsterPs.close();
            }
            if(insterSet !=null){
                insterSet.close();
            }
        }
    }


    public void close() {
        if (this.connection != null) {
            try {
                this.connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                this.connection = null;
            }
        }
    }

}