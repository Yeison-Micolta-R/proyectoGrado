/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mycompany.udenardbms;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import net.sf.jsqlparser.JSQLParserException;

/**
 *
 * @author yesec
 */
public class IA2 {

    private CountDownLatch latch;
    //StringBuilder r = null;
    String columname = "class";

    public IA2(CountDownLatch latch) {
        this.latch = latch;

    }

    public String recibo(String comand, String Rut) {
        StringBuilder r = new StringBuilder(); // No necesita inicializar en null
        String typeprimary = null;
        String columprimary = null;
        String[] rutas = null;
        String[] valuePrimary = null;

        try {
            r = CRUDParser.parser(comand);

            if (r.toString().toLowerCase().contains("null")) {
                return " ";
            }

            String[] parts = r.toString().split(",", 4);
            String rut = parts[0].replaceAll(RegrexExp.deleteCorchetes, "").trim();

            if (parts.length == 4) {
                String primary = parts[1].replaceAll(RegrexExp.deleteCorchetes, "").trim();
                typeprimary = parts[2].trim();
                columprimary = parts[3].trim();
                rutas = rut.split(";");
                valuePrimary = primary.split(";");

                this.latch = new CountDownLatch(1);
                initClient(rutas);

                latch.await(); // await ya maneja la excepción InterruptedException

                if (alter(comand)) {
                    if (update(comand, FunctionClass.clasesList, columprimary, FunctionClass.uniqueArray)) {
                        System.out.println("update exito");
                    }
                }

                comand = comand.replaceAll(RegrexExp.replaceWhere, columname)
                        .replaceAll("return", FunctionClass.recibo);

                return comand;
            } else {
                comand = comand.replaceAll(RegrexExp.replaceWhere, rut)
                        .replaceAll("return", FunctionClass.recibo);

                return comand;
            }

        } catch (JSQLParserException | InterruptedException ex) {
            Logger.getLogger(IA2.class.getName()).log(Level.SEVERE, null, ex);
        }

        return " ";
    }



    private String expressionnametable(String comand) {

        //String regex = "(?i)\\bfrom\\b\\s+([\\w]+)"; 
        String tableName = null;
        Pattern pattern = Pattern.compile(RegrexExp.nameTable);
        Matcher matcher = pattern.matcher(comand);
        if (matcher.find()) {
            tableName = matcher.group(1);

        } else {
            System.out.println("No se encontró el nombre de la tabla.");
        }
        return tableName;
        //return null;
    }

    private boolean alter(String comand) {
        StringBuilder r = null;// Simulating CRUDParser.parser(comand) method

        String[] clas = {"1"};
      //  System.err.println("leng " + clas.length);
        String nametable = expressionnametable(comand);

        for (String css : clas) {
            String newcomand = "ALTER TABLE " + nametable + " ADD COLUMN " + columname + " int class DEFAULT  -1;";
            try {
                r = CRUDParser.parser(newcomand);

            } catch (JSQLParserException ex) {
                Logger.getLogger(IA2.class.getName()).log(Level.SEVERE, null, ex);
            }

        }

        if (r.toString().contains("éxito")) {
            return true;
        }
        return false;

    }

    private boolean update(String comand, ArrayList<String> arrayclass, String columPrimary, String[] arrayvaluesprimary) {
        String nametable = expressionnametable(comand);
        StringBuilder r = new StringBuilder();
        boolean hasUpdates = false;
        int contador=0;
        //System.out.println("columnprymary"+ arrayvaluesprimary.toString());
        //System.out.println("columnprymary"+ columPrimary);
        StringBuilder commandBuilder = new StringBuilder();
        long starTime =System.currentTimeMillis();
        try {
            for (int i = 0; i < arrayclass.size(); i++) {
                // Construir el comando de actualización
                commandBuilder.setLength(0); // Limpiar el StringBuilder
                commandBuilder.append("UPDATE ")
                        .append(nametable)
                        .append(" SET ")
                        .append(columname)
                        .append(" = ")
                        .append(arrayclass.get(i))
                        .append(" WHERE ")
                        .append(columPrimary)
                        .append(" = '")
                        .append(arrayvaluesprimary[i].replaceAll(RegrexExp.deletellaves, "").trim()+"'");

               // System.out.println("update " + commandBuilder.toString());

                // Ejecutar el comando
                r = CRUDParser.parser(commandBuilder.toString());
                //System.out.println(" " + r);
                
                contador += FunctionClass.extraer_int(r.toString());
                // Verificar si alguna fila fue afectada
                if (r.toString().contains("afectadas")) {
                    hasUpdates = true;
                }
            }
            System.out.println("Filas Afectas-> "+ contador);
        } catch (JSQLParserException ex) {
            Logger.getLogger(IA2.class.getName()).log(Level.SEVERE, null, ex);
        }
        long endTime = System.currentTimeMillis();
        long elapsetTime = endTime - starTime;

        System.out.println("Tiempo de ejecucion IA2: " + elapsetTime + " msec.");//sumarse con el tiempo de consulta select

        return hasUpdates;
    }


    public void initClient(String[] rut) {
        boolean ident = false;
        try {
            IA iaInstance = new IA(latch, rut,ident);
            Thread hilo = new Thread(iaInstance);
            hilo.start();

        } catch (Exception e) {
            System.err.println("Error al iniciar el hilo: " + e.getMessage());
            e.printStackTrace();
        }

    }

}
