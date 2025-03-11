package com.cgr.base.application.rulesEngine.management.dto;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class listOptionsRG {
    private List<String> fechas;
    private List<String> trimestres;
    private List<EntidadDTO> entidades;
    private List<AmbitoDTO> ambitos;
    private List<FormularioDTO> formularios;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class EntidadDTO {
        private String codigo;
        private String nombre;
        private String ambito;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AmbitoDTO {
        private String codigo;
        private String nombre;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class FormularioDTO {
        private String codigo;
        private String nombre;
    }
}