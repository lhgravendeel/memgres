package com.memgres.standalone;

import com.memgres.core.Memgres;

import java.sql.SQLException;

public class ConnTester {

  public static void main(String[] args) throws Exception {
    Memgres.logAllStatements = true;
    Memgres memgres = Memgres.builder().port(40000).build().start();
    System.out.println("Got: " + memgres.getJdbcUrl() + " - " + memgres.getUser() + " - " + memgres.getPassword());
    for(int i = 0; i < 1000; i++) {
      Thread.sleep(1000_000);
    }
    // Found:
    // Statement: SET DateStyle=ISO
    //Statement: SET client_min_messages=notice
    //Statement: SELECT set_config('bytea_output','hex',false) FROM pg_show_all_settings() WHERE name = 'bytea_output'
    //Statement: SET client_encoding='utf-8'
  }

}
