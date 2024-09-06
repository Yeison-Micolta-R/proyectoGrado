/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mycompany.udenardbms;

/**
 *
 * @author Udenar
 */
public class Schema {
    String tablename,  columnname,  datatype,  spects;

    public Schema(String tablename, String columnname, String datatype, String spects) {
        this.tablename = tablename;
        this.columnname = columnname;
        this.datatype = datatype;
        this.spects = spects;
    }

    public String getColumnname() {
        return columnname;
    }
    
    
}
