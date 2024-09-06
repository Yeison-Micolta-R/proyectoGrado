/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mycompany.udenardbms;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;

/**
 *
 * @author yesec
 */
public class FunctionClass {

    static String recibo;
    static ArrayList<String> clasesList = new ArrayList<>();
    static String[] resultArray = null;
    static boolean status = false;
    static String[] uniqueArray = null;

    //Separa la cadena y reemplaza la columna y clase predicha 
    public static String splitcadenaa(String where, String parametro1, String parametro2, String parametro3) {

        String[] partes;

        // Usamos una expresión regular para dividir la cadena en tres partes
        if (where.contains(",")) {
            partes = where.split("(?<=" + parametro1 + ")|(?<=" + parametro2 + ")|(?<=" + parametro3 + ")", 4);
            partes[3] = partes[3].replace("'", "");
            partes[1] = " " + partes[3] + "=" + recibo;

            return partes[0] + partes[1];

        } else {
            partes = where.split("(?<=" + parametro1 + ")|(?<=" + parametro2 + ")", 3);

            return partes[0] + partes[1] + " return";/*recibo;*/
        }

    }

    public static String removelastCharacter(String where, String caracter) {
        // Dividir la cadena por el parámetro y conservar el delimitador

        String[] partes = where.split("(?<=" + caracter + ")", 2);

        if (partes.length > 1) {

            return partes[0].substring(0, partes[0].length() - 1);

        } else {
            return null; // Retorna cadena
        }

    }

    public static boolean Ruta(String where) {
        //String regex = ".*=\\s*[\"']?[a-zA-Z]:[/\\\\](?:[^/\\\\:*?\"<>|\\r\\n]+[/\\\\])*(?:[^/\\\\:*?\"<>|\\r\\n]+)?[\"']?.*";
        String regex = "=\\s*['\"]?([a-zA-Z]:[\\\\/][^'\"]*)['\"]?\\s*";

        // Expresión regular para encontrar una ruta de archivo (ajustada para Windows)
        //String regex = "[a-zA-Z]:[\\\\/][\\w\\\\/.-]+(?:[\\\\/][\\w\\\\/.-]+)*";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(where);
        boolean verda = false;

        if (matcher.find()) {

            UDENARDBMS.ruta = matcher.group(1);
            if (UDENARDBMS.ruta.contains(",")) {
                //  System.err.println("entre a coma");
                UDENARDBMS.ruta = removelastCharacter(UDENARDBMS.ruta, ",");
            }
            verda = true;
        }
        return verda;
    }

    public static void setClasepredicha(int clase) {

        recibo = String.valueOf(clase);

    }


    public static void resetClasepredicha() {
        recibo = null;
    }

    public static String[] uniqueArray(String[] originalArray) {
        // HashSet para almacenar valores únicos
        Set<String> uniqueValues = new HashSet<>();

        // Expresión regular para permitir solo caracteres válidos en la ruta
        Pattern validPattern = Pattern.compile("[^a-zA-Z0-9_\\:\\\\\\.\\/]");

        // Limpiar y agregar valores únicos del array original al HashSet
        for (String value : originalArray) {
            String cleanedValue = validPattern.matcher(value).replaceAll("");
            uniqueValues.add(cleanedValue);
        }
        uniqueArray = uniqueValues.toArray(new String[0]);
        // Convertir el HashSet a un array y retornar
        return uniqueArray;
    }
    /*
    public static String[] uniqueArray(String[] originalArray) {
        // System.out.println("llegue a unique");
        // Crear un HashSet para almacenar valores únicos
        Set<String> uniqueValues = new HashSet<>();

        //buscar el otro hassh
        // Agregar valores únicos del array original al HashSet
        for (String value : originalArray) {
            uniqueValues.add(value);
        }
        // Convertir el HashSet a un array si es necesario
        uniqueArray = uniqueValues.toArray(new String[0]);
        // System.out.println("Array con valores únicos:functionclass " + Arrays.toString(uniqueArray));

        return uniqueArray;

    }

     public static void uniqueray(String[] originalArray, String[] uniqueArray) {
        // Crear un nuevo array para almacenar los resultados
        resultArray = new String[originalArray.length];

        // Recorrer el array original y asignar valores al resultArray
        for (int i = 0; i < originalArray.length; i++) {
            for (int j = 0; j < uniqueArray.length; j++) {

                if (originalArray[i].equals(uniqueArray[j])) {

                    resultArray[i] = clasesList.get(j);

                }

            }
        }

    }*/
    public static Object splitcadenaa(String sql) {
        String[] parts = sql.split("DEFAULT", 2);

        // Comprobar que se hizo la división correctamente
        if (parts.length == 2) {
            // Obtener las partes antes y después de "DEFAULT"
            String beforeDefault = parts[0].trim();
            String afterDefault = parts[1].trim();
            Statement stmt;
            try {
                stmt = CCJSqlParserUtil.parse(beforeDefault);

                return new Object[]{stmt, afterDefault};
            } catch (JSQLParserException ex) {
                Logger.getLogger(FileManager.class.getName()).log(Level.SEVERE, null, ex);
            }

        } else {
            System.out.println("la casena no continee Default");
            try {
                Statement stmt = CCJSqlParserUtil.parse(sql);
                return new Object[]{stmt};
            } catch (JSQLParserException ex) {
                Logger.getLogger(FileManager.class.getName()).log(Level.SEVERE, null, ex);
            }

        }
        return null;

    }

    public static Integer extraer_int(String input) {

        int number = 0;
        // Define una expresión regular para encontrar números en la cadena
        Pattern pattern = Pattern.compile("\\d+");
        Matcher matcher = pattern.matcher(input);

        if (matcher.find()) {
            String numberStr = matcher.group();
            number = Integer.parseInt(numberStr);
           //System.out.println("Número extraído: " + number);
        }

        return number;
    }

}
