package com.cgr.base.application.rulesEngine.management.dto;
import java.util.List;

import com.cgr.base.application.rulesEngine.management.dto.listOptionsRG.AmbitoDTO;
import com.cgr.base.application.rulesEngine.management.dto.listOptionsRG.EntidadDTO;
import com.cgr.base.application.rulesEngine.management.dto.listOptionsRG.FormularioDTO;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class listOptionsEG {

    private List<String> fechas;
    private List<String> trimestres;
    private List<EntidadDTO> entidades;
    private List<AmbitoDTO> ambitos;
    private List<FormularioDTO> reportes;
    
}
