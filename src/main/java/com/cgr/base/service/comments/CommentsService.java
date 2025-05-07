package com.cgr.base.service.comments;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;

@Service
public class CommentsService {

        private static final Logger logger = LoggerFactory.getLogger(CommentsService.class);

        @PersistenceContext
        private EntityManager entityManager;

        @Transactional
        public void createComment(Map<String, Object> commentData, Long userId, String userName, List<String> roles) {
                logger.info("Creating comment with data: {}", commentData);
                logger.info("User ID: {}, User Name: {}, Roles: {}", userId, userName, roles);

                // Validate required fields
                if (commentData.get("fecha") == null ||
                                commentData.get("codigoEntidad") == null ||
                                commentData.get("tipoComent") == null ||
                                commentData.get("comentario") == null ||
                                commentData.get("prioridad") == null ||
                                commentData.get("categoria") == null) {
                        throw new IllegalArgumentException("Missing required fields in commentData.");
                }

                try {
                        // Insert the comment into the database
                        String sql = """
                                        INSERT INTO COMENTARIOS (
                                            FECHA, CODIGO_ENTIDAD, TIPO_COMENT, COMENTARIO,
                                            PRIORIDAD, CATEGORIA, FECHA_HORA_COMENTARIO,
                                            ROL_USUARIO, NOMBRE_USUARIO
                                        )
                                        VALUES (
                                            :fecha, :codigoEntidad, :tipoComent, :comentario,
                                            :prioridad, :categoria, GETDATE(),
                                            :rolUsuario, :nombreUsuario
                                        )
                                        """;

                        String rolesString = String.join(",", roles);

                        entityManager.createNativeQuery(sql)
                                        .setParameter("fecha", commentData.get("fecha"))
                                        .setParameter("codigoEntidad", commentData.get("codigoEntidad"))
                                        .setParameter("tipoComent", commentData.get("tipoComent"))
                                        .setParameter("comentario", commentData.get("comentario"))
                                        .setParameter("prioridad", commentData.get("prioridad"))
                                        .setParameter("categoria", commentData.get("categoria"))
                                        .setParameter("rolUsuario", rolesString)
                                        .setParameter("nombreUsuario", userName)
                                        .executeUpdate();

                        logger.info("Comment inserted successfully.");
                } catch (Exception e) {
                        logger.error("Error executing SQL query: {}", e.getMessage(), e);
                        // No lanzar excepción genérica aquí, solo registrar el error
                }
        }

        public List<Map<String, Object>> getComments(int fecha, String codigoEntidad, int tipoComent) {
                logger.info("Fetching comments for Fecha: {}, CodigoEntidad: {}, TipoComent: {}", fecha, codigoEntidad,
                                tipoComent);

                String sql = """
                                SELECT
                                    c.COMENTARIO,
                                    p.TEXTO_PRIORIDAD AS PRIORIDAD,
                                    cat.TEXTO_CATEGORIA AS CATEGORIA,
                                    c.ROL_USUARIO,
                                    c.NOMBRE_USUARIO,
                                    FORMAT(c.FECHA_HORA_COMENTARIO, 'yyyy-MM-dd HH:mm:ss') AS FECHA_HORA_COMENTARIO
                                FROM COMENTARIOS c
                                JOIN PRIORIDADES p ON c.PRIORIDAD = p.ID
                                JOIN CATEGORIA cat ON c.CATEGORIA = cat.ID
                                WHERE c.FECHA = :fecha
                                  AND c.CODIGO_ENTIDAD = :codigoEntidad
                                  AND c.TIPO_COMENT = :tipoComent
                                ORDER BY c.FECHA_HORA_COMENTARIO DESC
                                """;

                try {
                        return entityManager.createNativeQuery(sql, Map.class)
                                        .setParameter("fecha", fecha)
                                        .setParameter("codigoEntidad", codigoEntidad)
                                        .setParameter("tipoComent", tipoComent)
                                        .getResultList();
                } catch (Exception e) {
                        logger.error("Error fetching comments: {}", e.getMessage(), e);
                        throw new RuntimeException("Error fetching comments: " + e.getMessage(), e);
                }
        }
}
