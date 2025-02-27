package com.cgr.base.application.maps.entity.departments;

import com.cgr.base.application.maps.entity.municipalities.EntityMunicipality;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Entity
@Table ( name = "departments")
public class EntityDepartments {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long department_id;
    private String id;
    private Integer gid;
    private String dpto_ccdgo;
    private String dpto_cnmbr;
    private  Integer dpto_ano_c;
    private String dpto_act_a;
    private Float  dpto_narea;
    private Integer dpto_csmbl;
    private Integer dpto_vgnc;
    private Float shape_leng;
    private Float shape_area;
    private String geometry;
    private String image;


    @OneToMany(mappedBy = "department", fetch = FetchType.EAGER)
    private List<EntityMunicipality> municipalitie;
}