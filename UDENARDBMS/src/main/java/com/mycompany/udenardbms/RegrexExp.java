/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mycompany.udenardbms;

/**
 *
 * @author yesec
 */
public class RegrexExp {

    //static String iRuta, deleteCorchetes, replaceWhere, nameTable;

    static String idRuta = "=\\s*['\"]?([a-zA-Z]:[\\\\/][^'\"])['\"]?\\s"; 
    static String deleteCorchetes = "[\\[\\]]"; 
    static String deleteparentesis = "IA+([\\(]+)"; 
    static String deletellaves = "[\\{\\}]"; 
    static String replaceWhere = "(?<=where\\s)\\w+(?=\\s*=)";
    static String nameTable = "(?i)\\bfrom\\b\\s+([\\w]+)"; 
    
    public static boolean identURL(String Cadena) {
        String regex = "^[a-zA-Z]:/(?:[^/:?\"<>|\\r\\n]+/)(?:[^/:*?\"<>|\\r\\n]+)?$";
        return Cadena.matches(regex);
    }

}
