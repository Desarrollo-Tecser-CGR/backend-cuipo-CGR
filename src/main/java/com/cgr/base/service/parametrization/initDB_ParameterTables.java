package com.cgr.base.service.parametrization;

import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.stereotype.Service;

import com.cgr.base.service.parametrization.InitParametrization.dataCategoryInit;
import com.cgr.base.service.parametrization.InitParametrization.dataParameterInit;
import com.cgr.base.service.parametrization.InitParametrization.generalTablesInit;
import com.cgr.base.service.parametrization.InitParametrization.specificTablesInit;

@Service
public class initDB_ParameterTables {

    @Autowired
    private dataCategoryInit dataCategoryInit;

    @Autowired
    private generalTablesInit generalParameter;

    @Autowired
    private specificTablesInit specificParameter;

    @Autowired
    private dataParameterInit dataParameterInit;

    public void executeInitTables() {
        dataCategoryInit.initCategoryTable();
        generalParameter.initGeneralTables();
        specificParameter.initCategoryTable();
        dataParameterInit.processTablesSource();
    }

}
