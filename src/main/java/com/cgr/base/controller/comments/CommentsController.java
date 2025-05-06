package com.cgr.base.controller.comments;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import com.cgr.base.config.abstractResponse.AbstractController;
import com.cgr.base.config.jwt.JwtService;
import com.cgr.base.service.comments.CommentsService;

import jakarta.servlet.http.HttpServletRequest;

@RestController
@RequestMapping("/api/v1/comments")
public class CommentsController extends AbstractController {

    @Autowired
    private CommentsService commentsService;

    @Autowired
    private JwtService jwtService;

    @PreAuthorize("hasAuthority('MENU_COMMENTS')")
    @PostMapping
    public ResponseEntity<?> createComment(@RequestBody Map<String, Object> commentData, HttpServletRequest request) {
        String header = request.getHeader(HttpHeaders.AUTHORIZATION);
        String token = header.split(" ")[1];
        Long userId = jwtService.extractUserIdFromToken(token);

        if (userId == null) {
            return requestResponse(null, "User ID not found.", HttpStatus.FORBIDDEN, false);
        }

        List<String> roles = jwtService.getRolesToken(token);
        String userName = jwtService.getClaimUserName(token);

        try {
            commentsService.createComment(commentData, userId, userName, roles);
            return requestResponse(null, "Comment created successfully.", HttpStatus.CREATED, true);
        } catch (Exception e) {
            return requestResponse(null, "Error creating comment: " + e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR,
                    false);
        }
    }

    @PreAuthorize("hasAuthority('MENU_COMMENTS')")
    @GetMapping("/{fecha}/{codigoEntidad}/{nombreEntidad}")
    public ResponseEntity<?> getComments(@PathVariable int fecha, @PathVariable String codigoEntidad,
            @PathVariable String nombreEntidad) {
        try {
            List<Map<String, Object>> comments = commentsService.getComments(fecha, codigoEntidad, nombreEntidad);
            return requestResponse(comments, "Comments retrieved successfully.", HttpStatus.OK, true);
        } catch (Exception e) {
            return requestResponse(null, "Error retrieving comments: " + e.getMessage(),
                    HttpStatus.INTERNAL_SERVER_ERROR, false);
        }
    }

    @PreAuthorize("hasAuthority('MENU_COMMENTS')")
    @PutMapping
    public ResponseEntity<?> updateComment(@RequestBody Map<String, Object> commentData, HttpServletRequest request) {
        String header = request.getHeader(HttpHeaders.AUTHORIZATION);
        String token = header.split(" ")[1];
        Long userId = jwtService.extractUserIdFromToken(token);

        if (userId == null) {
            return requestResponse(null, "User ID not found.", HttpStatus.FORBIDDEN, false);
        }

        try {
            commentsService.updateComment(commentData, userId);
            return requestResponse(null, "Comment updated successfully.", HttpStatus.OK, true);
        } catch (Exception e) {
            return requestResponse(null, "Error updating comment: " + e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR,
                    false);
        }
    }

    @PreAuthorize("hasAuthority('MENU_COMMENTS')")
    @DeleteMapping
    public ResponseEntity<?> deleteComment(@RequestBody Map<String, Object> commentData) {
        try {
            commentsService.deleteComment(commentData);
            return requestResponse(null, "Comment deleted successfully.", HttpStatus.OK, true);
        } catch (Exception e) {
            return requestResponse(null, "Error deleting comment: " + e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR,
                    false);
        }
    }
}
