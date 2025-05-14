package com.cgr.base.controller.notificactions;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.cgr.base.config.abstractResponse.AbstractController;
import com.cgr.base.config.jwt.JwtService;
import com.cgr.base.entity.notifications.NotificationUserEntity;
import com.cgr.base.service.notifications.NotificationsService;

import jakarta.servlet.http.HttpServletRequest;

@RestController
@RequestMapping("/api/v1/notifications")
public class NotificationController extends AbstractController {

    private final NotificationsService notificationService;

    @Autowired
    private JwtService jwtService;

    public NotificationController(NotificationsService notificationService) {
        this.notificationService = notificationService;
    }

    @GetMapping()
    public ResponseEntity<?> getNotifications(HttpServletRequest request) {
        String header = request.getHeader(HttpHeaders.AUTHORIZATION);
        if (header == null || !header.startsWith("Bearer ")) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body("Falta token de autorización.");
        }
        String token = header.split(" ")[1];
        Long userId = jwtService.extractUserIdFromToken(token);

        if (userId == null) {
            return requestResponse(null, "User ID not found.", HttpStatus.FORBIDDEN, false);
        }

        List<NotificationUserEntity> notifications = notificationService.getNotificationsForUser(userId);
        return ResponseEntity.ok(notifications);
    }

    @GetMapping("/mark-as-read/")
    public ResponseEntity<?> markAsRead(HttpServletRequest request,
            @RequestParam("notificationId") Long notificationId) {

        String header = request.getHeader(HttpHeaders.AUTHORIZATION);
        String token = header.split(" ")[1];

        Long userId = jwtService.extractUserIdFromToken(token);

        if (userId == null) {
            return requestResponse(null, "User ID not found.", HttpStatus.FORBIDDEN, false);
        }
        notificationService.markNotificationAsRead(userId, notificationId);

        return ResponseEntity.ok().body(notificationId);
    }

    @GetMapping("/mark-all-as-read")
    public ResponseEntity<?> markAllAsRead(HttpServletRequest request) {
        String header = request.getHeader(HttpHeaders.AUTHORIZATION);
        if (header == null || !header.startsWith("Bearer ")) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body("Falta token de autorización.");
        }

        String token = header.split(" ")[1];
        Long userId = jwtService.extractUserIdFromToken(token);

        if (userId == null) {
            return requestResponse(null, "User ID not found.", HttpStatus.FORBIDDEN, false);
        }

        notificationService.markAllNotificationsAsRead(userId);

        return ResponseEntity.ok().build();
    }

}