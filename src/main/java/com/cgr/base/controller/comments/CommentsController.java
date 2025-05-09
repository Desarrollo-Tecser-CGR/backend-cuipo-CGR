package com.cgr.base.controller.comments;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import com.cgr.base.config.abstractResponse.AbstractController;
import com.cgr.base.config.jwt.JwtService;
import com.cgr.base.repository.user.IUserRepositoryJpa;
import com.cgr.base.service.comments.CommentsService;

import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

@RestController
@RequestMapping("/api/v1/comments")
@PreAuthorize("hasAuthority('MENU_CERTIFY')")
public class CommentsController extends AbstractController {

    private static final Logger logger = LoggerFactory.getLogger(CommentsController.class);

    @Autowired
    private CommentsService commentsService;

    @Autowired
    private IUserRepositoryJpa userRepositoryJpa;

    @PostMapping
    public ResponseEntity<?> createComment(@RequestBody Map<String, Object> commentData, HttpServletRequest request) {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();

        String userName = authentication.getName();

        List<String> roles = userRepositoryJpa.findBySAMAccountNameWithRoles(userName)
                .map(user -> user.getRoles().stream().map(role -> role.getName()).collect(Collectors.toList()))
                .orElseThrow(() -> new RuntimeException("User not found or roles not assigned."));

        System.out.println(userName);
        System.out.println(roles);

        Map<String, Object> result = commentsService.createComment(commentData, userName, roles);
        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of(
                "data", result,
                "message", "Comment created successfully.",
                "status", HttpStatus.CREATED.value(),
                "successful", true));
    }

    @GetMapping("/{fecha}/{codigoEntidad}/{tipoComent}")
    public ResponseEntity<?> getComments(@PathVariable int fecha, @PathVariable String codigoEntidad,
            @PathVariable int tipoComent) {
        List<Map<String, Object>> comments = commentsService.getComments(fecha, codigoEntidad, tipoComent);
        return ResponseEntity.ok(Map.of(
                "data", comments,
                "message", "Comments retrieved successfully.",
                "status", HttpStatus.OK.value(),
                "successful", true));
    }
}
