package com.cgr.base.service.comments;

import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Service;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;

@Service
public class CommentsService {

        @PersistenceContext
        private EntityManager entityManager;

        public void createComment(Map<String, Object> commentData, Long userId, String userName, List<String> roles) {
                String sql = """
                                INSERT INTO COMENTARIOS (FECHA, CODIGO_ENTIDAD, NOMBRE_ENTIDAD, COMENTARIO, ROL_USUARIO, NOMBRE_USUARIO,
                                CATEGORIA_COMENTARIO, PRIORIDAD_COMENTARIO)
                                VALUES (:fecha, :codigoEntidad, :nombreEntidad, :comentario, :rolUsuario, :nombreUsuario,
                                :categoriaComentario, :prioridadComentario)
                                """;

                String rolesString = String.join(",", roles);

                entityManager.createNativeQuery(sql)
                                .setParameter("fecha", commentData.get("fecha"))
                                .setParameter("codigoEntidad", commentData.get("codigoEntidad"))
                                .setParameter("nombreEntidad", commentData.get("nombreEntidad"))
                                .setParameter("comentario", commentData.get("comentario"))
                                .setParameter("rolUsuario", rolesString) // Save roles in the database
                                .setParameter("nombreUsuario", userName) // Use userName from token
                                .setParameter("categoriaComentario", commentData.get("categoriaComentario"))
                                .setParameter("prioridadComentario", commentData.get("prioridadComentario"))
                                .executeUpdate();
        }

        public List<Map<String, Object>> getComments(int fecha, String codigoEntidad, String nombreEntidad) {
                String sql = """
                                SELECT * FROM COMENTARIOS
                                WHERE FECHA = :fecha AND CODIGO_ENTIDAD = :codigoEntidad AND NOMBRE_ENTIDAD = :nombreEntidad
                                ORDER BY FECHA_HORA_COMENTARIO DESC
                                """;

                return entityManager.createNativeQuery(sql, Map.class)
                                .setParameter("fecha", fecha)
                                .setParameter("codigoEntidad", codigoEntidad)
                                .setParameter("nombreEntidad", nombreEntidad)
                                .getResultList();
        }

        public void updateComment(Map<String, Object> commentData, Long userId) {
                String sql = """
                                UPDATE COMENTARIOS
                                SET COMENTARIO = :comentario, CATEGORIA_COMENTARIO = :categoriaComentario,
                                PRIORIDAD_COMENTARIO = :prioridadComentario
                                WHERE FECHA = :fecha AND CODIGO_ENTIDAD = :codigoEntidad AND NOMBRE_ENTIDAD = :nombreEntidad
                                AND FECHA_HORA_COMENTARIO = :fechaHoraComentario
                                """;

                entityManager.createNativeQuery(sql)
                                .setParameter("comentario", commentData.get("comentario"))
                                .setParameter("categoriaComentario", commentData.get("categoriaComentario"))
                                .setParameter("prioridadComentario", commentData.get("prioridadComentario"))
                                .setParameter("fecha", commentData.get("fecha"))
                                .setParameter("codigoEntidad", commentData.get("codigoEntidad"))
                                .setParameter("nombreEntidad", commentData.get("nombreEntidad"))
                                .setParameter("fechaHoraComentario", commentData.get("fechaHoraComentario"))
                                .executeUpdate();
        }

        public void deleteComment(Map<String, Object> commentData) {
                String sql = """
                                DELETE FROM COMENTARIOS
                                WHERE FECHA = :fecha AND CODIGO_ENTIDAD = :codigoEntidad AND NOMBRE_ENTIDAD = :nombreEntidad
                                AND FECHA_HORA_COMENTARIO = :fechaHoraComentario
                                """;

                entityManager.createNativeQuery(sql)
                                .setParameter("fecha", commentData.get("fecha"))
                                .setParameter("codigoEntidad", commentData.get("codigoEntidad"))
                                .setParameter("nombreEntidad", commentData.get("nombreEntidad"))
                                .setParameter("fechaHoraComentario", commentData.get("fechaHoraComentario"))
                                .executeUpdate();
        }
}
