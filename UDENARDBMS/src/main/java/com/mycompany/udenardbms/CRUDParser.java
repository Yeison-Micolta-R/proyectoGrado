/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mycompany.udenardbms;

import java.util.List;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitorAdapter;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.FromItemVisitorAdapter;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.update.Update;

/**
 *
 * @author Udenar
 */
public class CRUDParser {

    public static String tablename = "";
    public static List<SelectItem> projectionlist;
    public static Expression filterList;
    public static boolean ruta;
    public static EqualsTo recibo;

    public static StringBuilder parser(String sql) throws JSQLParserException {
        FileManager fm = new FileManager();
        FunctionClass Fc = new FunctionClass();
        Statement stmt = CCJSqlParserUtil.parse(sql);

        if (stmt instanceof CreateTable) {
            CreateTable create = (CreateTable) stmt;
            try {
                tablename = create.getTable().getName();
            } catch (Exception e) {
                e.printStackTrace();
            }
            fm.createTable(tablename);
            List<ColumnDefinition> lst = create.getColumnDefinitions();
            for (ColumnDefinition columnDefinition : lst) {
                fm.writeTableSchema(tablename,
                        columnDefinition.getColumnName(),
                        columnDefinition.getColDataType() + "",
                        columnDefinition.getColumnSpecs() + "");
            }
            fm.closeTableSchema();
            return new StringBuilder("CREATE " + tablename + " succes");

        } else if (stmt instanceof Insert insert) {
            
            tablename = insert.getTable().getName();
            List<Column> lsti = insert.getColumns();
            if (lsti != null) {
                for (Column column : lsti) {
                    System.out.println(column.getColumnName());
                }
            }
            ItemsList il = insert.getItemsList();
            il.accept(new ItemsListVisitorAdapter() {
                @Override
                public void visit(ExpressionList el) {

                     fm.insert(tablename, el.getExpressions());

                }
            });

            return new StringBuilder("INSERT " + tablename + " succes");

        } else if (stmt instanceof Select) {
            long starTime =System.currentTimeMillis();
            Select select = (Select) stmt;
            PlainSelect body = (PlainSelect) select.getSelectBody();
            try {
                body.getFromItem().accept(new FromItemVisitorAdapter() {
                    @Override
                    public void visit(Table table) {
                        tablename = table.getName();
                    }
                });
                projectionlist = body.getSelectItems();
                filterList = body.getWhere();              
                //System.out.println("filterList = " + filterList);
                StringBuilder sb = fm.select(tablename, projectionlist, filterList);
                long endTime = System.currentTimeMillis();
                long elapsetTime = endTime - starTime;
                
                System.out.println("Tiempo de ejecucion: " + elapsetTime + " msec.");
                return sb ;
            } catch (Exception e) {
                
                e.printStackTrace();
                return null;
            }

        } else if (stmt instanceof Update) {
            Update update = (Update) stmt;
            tablename = update.getTable().getName();
            List<Expression> eu = update.getExpressions();
            // System.err.println("exxx-> " + eu);
            
            for (int i = 0; i < eu.size(); i++) {
                
                try {
                    int ev = (int) SimpleEvaluateExpr.evaluate(eu.get(i).toString());
                    //System.err.println("ev " + ev);
                    Expression ex = CCJSqlParserUtil.parseExpression(String.valueOf(ev));
                    // System.err.println("ex " + ex);
                    eu.set(i, ex);
                } catch (Exception e) {
                    System.err.println("error antes de llegar a fm update");
                }

            }
            List<Column> columns = update.getColumns();
            Expression where = update.getWhere();
            
            return fm.update(tablename, columns, eu, where);

        } else if (stmt instanceof Delete) {
            Delete delete = (Delete) stmt;
            String tablename = delete.getTable().getName();
            Expression where = delete.getWhere();

            if (tablename == null || tablename.isEmpty()) {
                System.err.println("Error: Table name is null or empty.");
                return null;
            }

            try {
                StringBuilder result = fm.delete(tablename, where);
                if (result == null) {
                    System.err.println("Delete operation failed for table " + tablename);
                } else {
                    System.out.println("Delete operation succeeded for table " + tablename);
                }
                return result;
            } catch (Exception e) {
                System.err.println("Delete operation failed for table " + tablename + ": " + e.getMessage());
                e.printStackTrace();
                return null;
            }
        } else if (stmt instanceof Alter) {
            Object[] result = (Object[]) FunctionClass.splitcadenaa(sql);
            stmt = (Statement) result[0];
            Alter alter = (Alter) stmt;
            try {
                tablename = alter.getTable().getName();
                List<AlterExpression> alterExpressions = alter.getAlterExpressions();
                for (AlterExpression alterExpression : alterExpressions) {
                    switch (alterExpression.getOperation()) {
                        case ADD:
                            // Supongamos que alterExpression.getColDataTypeList() devuelve una lista de ColumnDefinition
                            List<AlterExpression.ColumnDataType> addColumns = alterExpression.getColDataTypeList();
                            Object defaul = null;
                            for (ColumnDefinition columnDefinition : addColumns) {
                                if (!fm.doesColumnExist(tablename, columnDefinition.getColumnName())) {
                                    fm.openTableSchema();
                                    fm.writeTableSchema(tablename, columnDefinition.getColumnName(), columnDefinition.getColDataType() + "", columnDefinition.getColumnSpecs() + "");

                                    if (result.length > 1) {
                                        String data = (String) result[1];

                                        data = Fc.removelastCharacter(data, ";");

                                        if (columnDefinition.getColDataType().toString().toLowerCase().contains("varchar") && data.charAt(0) == '\''
                                                && data.charAt(data.length() - 1) == '\'') {
                                            String exp = data;
                                            int t = exp.length();
                                            defaul = exp.substring(1, t - 1);

                                        } else if (columnDefinition.getColDataType().toString().toLowerCase().contains("int")) {
                                            defaul = Integer.parseInt(data.trim() + "");
                                        } else if (columnDefinition.getColDataType().toString().toLowerCase().contains("float")) {
                                            defaul = Float.parseFloat(data.trim() + "");
                                        } else {
                                            return new StringBuilder("ERROR: ESCRIBE UN VALOR POR DEFECTO VALIDO  ");
                                        }

                                    } else {
                                        if (columnDefinition.getColDataType().toString().toLowerCase().contains("varchar")) {
                                            defaul = "";
                                        } else if (columnDefinition.getColDataType().toString().toLowerCase().contains("int")) {
                                            defaul = 0;
                                        } else if (columnDefinition.getColDataType().toString().toLowerCase().contains("float")) {
                                            defaul = (float) 0.0f;
                                        }
                                    }

                                    fm.addColumn(tablename, columnDefinition.getColumnName(), columnDefinition.getColDataType() + "", new Object[]{defaul});
                                    //System.out.println("tamañooo->  " );
                                    return new StringBuilder("ALTER " + tablename + " éxito");
                                } else {

                                    return new StringBuilder("ERROR ALTER: " + "YA EXISTE " + "COLUMNA " + columnDefinition.getColumnName() + " EN LA TABLA " + tablename);
                                }
                            }
                            break;

                        case DROP:
                            String dropColumnName = alterExpression.getColumnName();

                            break;
                        case MODIFY:

                            break;
                        default:
                            throw new UnsupportedOperationException("Operación no soportada: " + alterExpression.getOperation());
                    }
                }
                fm.closeTableSchema();
                return new StringBuilder("ALTER " + tablename + " éxito");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return new StringBuilder("Comand Unknow");
    }
}
