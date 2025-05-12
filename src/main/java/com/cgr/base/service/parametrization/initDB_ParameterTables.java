package com.cgr.base.service.parametrization;

import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.stereotype.Service;

import com.cgr.base.service.parametrization.InitParametrization.dataCategoryInit;

@Service
public class initDB_ParameterTables {

    @Autowired
    private dataCategoryInit categorias;

    public void executeInitTables() {
        categorias.initCategoryTable();
    }

}
