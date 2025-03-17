package com.cgr.base.application.maps.entity.municipalities;

import com.cgr.base.application.maps.entity.departments.EntityDepartments;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Entity
@Table( name = "municipalities")
public class EntityMunicipality {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer municipality_id;;
    private String id;
    private Integer gid;
    private String dpto_ccdgo;
    private String mpio_ccdgo;
    private String mpio_cnmbr;
    private  String mpio_crslc;
    private Float mpio_narea;
    private String mpio_ccnct;
    private Integer mpio_nano;
    private String dpto_cnmbr;
    private Float shape_leng;
    private Float shape_area;

    @Column(name = "geometry", columnDefinition = "VARCHAR(MAX)")
    private String geometry;

    private String image;
    private Integer department_id;

    @ManyToOne
    @JoinColumn(name = "dpto_ccdgo", referencedColumnName = "dpto_ccdgo", insertable = false, updatable = false)
    private EntityDepartments department;

}
