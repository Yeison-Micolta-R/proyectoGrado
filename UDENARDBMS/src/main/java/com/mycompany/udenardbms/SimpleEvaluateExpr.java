/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mycompany.udenardbms;

import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

/**
 *
 * @author Udenar
 */
public class SimpleEvaluateExpr {

    static double evaluate(String expr) throws JSQLParserException {
        final Stack<Object> stack = new Stack<>();
        Expression parseExpression = null;
        try {
          //  System.out.println("expr = " + expr);
            parseExpression = CCJSqlParserUtil.parseCondExpression(expr);
        } catch (JSQLParserException e) {

            e.printStackTrace();
            throw e;
        }

        ExpressionDeParser deparser = new ExpressionDeParser() {

            private void debugStack(String operation) {
           
            }

            @Override
            public void visit(Function function) {
                super.visit(function);
                double o1 = (double) stack.pop();
                debugStack("pop in Function");
                double rad = Math.toRadians(o1);
                switch (function.getName().toLowerCase()) {
                    case "sin":
                        stack.push(Math.sin(rad));
                        break;
                    case "cos":
                        stack.push(Math.cos(rad));
                        break;
                }
                debugStack("push in Function");
            }

            @Override
            public void visit(Between between) {
                super.visit(between);
                double l2 = (double) stack.pop();
                double l1 = (double) stack.pop();
                double o = (double) stack.pop();
                debugStack("pop in Between");
                stack.push(o >= l1 && o <= l2 ? 1. : 0.);
                debugStack("push in Between");
            }

            @Override
            public void visit(LikeExpression likeExpression) {
                super.visit(likeExpression);
                String plantilla = likeExpression.getRightExpression().toString().replace("%", ".*");
                Pattern pat = Pattern.compile(plantilla);
                Matcher mat = pat.matcher(likeExpression.getLeftExpression().toString());
                stack.push(mat.matches() ? 1. : 0.);
                debugStack("push in LikeExpression");
            }

            @Override
            public void visit(Addition addition) {
                super.visit(addition);
                double o1 = (double) stack.pop();
                double o2 = (double) stack.pop();
                debugStack("pop in Addition");
                stack.push(o2 + o1);
                debugStack("push in Addition");
            }

            @Override
            public void visit(Multiplication multiplication) {
                super.visit(multiplication);
                double o1 = (double) stack.pop();
                double o2 = (double) stack.pop();
                debugStack("pop in Multiplication");
                stack.push(o2 * o1);
                debugStack("push in Multiplication");
            }

            @Override
            public void visit(Division division) {
                super.visit(division);
                double o1 = (double) stack.pop();
                double o2 = (double) stack.pop();
                debugStack("pop in Division");
                stack.push(o2 / o1);
                debugStack("push in Division");
            }

            @Override
            public void visit(Subtraction subtraction) {
                super.visit(subtraction);
                double fac1 = (double) stack.pop();
                double fac2 = (double) stack.pop();
                debugStack("pop in Subtraction");
                stack.push(fac2 - fac1);
                debugStack("push in Subtraction");
            }

            @Override
            public void visit(LongValue longValue) {
                super.visit(longValue);
                stack.push((double) longValue.getValue());
                debugStack("push in LongValue");
            }

            @Override
            public void visit(DoubleValue doubleValue) {
                super.visit(doubleValue);
                stack.push(doubleValue.getValue());
                debugStack("push in DoubleValue");
            }

            @Override
            public void visit(EqualsTo equalsTo) {
                super.visit(equalsTo);
              
                Object o1 = stack.pop();
               
                Object o2 = stack.pop();
                
                //debugStack("pop in EqualsTo");
                if (o1 instanceof String && o2 instanceof String) {
                    stack.push(o1.equals(o2) ? 1. : 0.);
                } else if(o1 instanceof Number && o2 instanceof Number) {
                    stack.push(((Number) o2).doubleValue() == ((Number) o1).doubleValue() ? 1. : 0.);
                }else{
                    stack.push(0.);
                }
                debugStack("push in EqualsTo");
            }

            @Override
            public void visit(GreaterThan greaterThan) {
                super.visit(greaterThan);
                Object o1 = stack.pop();
                Object o2 = stack.pop();
                debugStack("pop in GreaterThan");
                if (o1 instanceof String && o2 instanceof String) {
                    stack.push(((String) o2).compareTo((String) o1) > 0 ? 1. : 0.);
                } else {
                    stack.push(((Number) o2).doubleValue() > ((Number) o1).doubleValue() ? 1. : 0.);
                }
                debugStack("push in GreaterThan");
            }

            @Override
            public void visit(GreaterThanEquals greaterThanEquals) {
                super.visit(greaterThanEquals);
                Object o1 = stack.pop();
                Object o2 = stack.pop();
                debugStack("pop in GreaterThanEquals");
                if (o1 instanceof String && o2 instanceof String) {
                    stack.push(((String) o2).compareTo((String) o1) >= 0 ? 1. : 0.);
                } else {
                    stack.push(((Number) o2).doubleValue() >= ((Number) o1).doubleValue() ? 1. : 0.);
                }
                debugStack("push in GreaterThanEquals");
            }

            @Override
            public void visit(MinorThan minorThan) {
                super.visit(minorThan);
                Object o1 = stack.pop();
                Object o2 = stack.pop();
                debugStack("pop in MinorThan");
                if (o1 instanceof String && o2 instanceof String) {
                    stack.push(((String) o2).compareTo((String) o1) < 0 ? 1. : 0.);
                } else {
                    stack.push(((Number) o2).doubleValue() < ((Number) o1).doubleValue() ? 1. : 0.);
                }
                debugStack("push in MinorThan");
            }

            @Override
            public void visit(MinorThanEquals minorThanEquals) {
                super.visit(minorThanEquals);
                Object o1 = stack.pop();
                Object o2 = stack.pop();
                debugStack("pop in MinorThanEquals");
                if (o1 instanceof String && o2 instanceof String) {
                    stack.push(((String) o2).compareTo((String) o1) <= 0 ? 1. : 0.);
                } else {
                    stack.push(((Number) o2).doubleValue() <= ((Number) o1).doubleValue() ? 1. : 0.);
                }
                debugStack("push in MinorThanEquals");
            }

            @Override
            public void visit(AndExpression andExpression) {
                super.visit(andExpression);
                double o1 = (double) stack.pop();
                double o2 = (double) stack.pop();
                debugStack("pop in AndExpression");
                stack.push(o1 == 1 && o2 == 1 ? 1. : 0.);
                debugStack("push in AndExpression");
            }

            @Override
            public void visit(OrExpression orExpression) {
                super.visit(orExpression);
                double o1 = (double) stack.pop();
                double o2 = (double) stack.pop();
                debugStack("pop in OrExpression");
                stack.push(o1 == 1 || o2 == 1 ? 1. : 0.);
                debugStack("push in OrExpression");
            }

            @Override
            public void visit(StringValue stringValue) {
                super.visit(stringValue);
                stack.push(stringValue.getValue());
                debugStack("push in StringValue");
            }
        };

        StringBuilder b = new StringBuilder();
        deparser.setBuffer(b);
        parseExpression.accept(deparser);

        if (stack.isEmpty()) {
           // System.err.println("Stack is empty after evaluation!");
            throw new JSQLParserException("Evaluation stack is empty");
        }

        return (double) stack.pop();
    }
}
