package com.cgr.base.service.parametrization;

import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.stereotype.Service;

import com.cgr.base.service.parametrization.InitParametrization.dataCategoryInit;
import com.cgr.base.service.parametrization.InitParametrization.dataIcldAccountInit;
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

    @Autowired
    private dataIcldAccountInit dataIcldAccountInit;

    public void executeInitTables() {
        dataCategoryInit.initCategoryTable();
        generalParameter.initGeneralTables();
        specificParameter.initCategoryTable();
        dataIcldAccountInit.tableCuentaICLD();
        dataParameterInit.processTablesSource();
    }

}
