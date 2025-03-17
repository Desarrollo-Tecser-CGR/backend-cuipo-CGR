package com.cgr.base.application.services.permission;

import com.cgr.base.domain.models.entity.Logs.UserEntity;
import com.cgr.base.infrastructure.repositories.repositories.user.IUserRepositoryJpa;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Service;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class UserDetailsServiceImpl implements UserDetailsService {

    private final IUserRepositoryJpa usuarioRepository;

    public UserDetailsServiceImpl(IUserRepositoryJpa usuarioRepository) {
        this.usuarioRepository = usuarioRepository;
    }

    @Override
    public UserDetails loadUserByUsername(String UserEntity) throws UsernameNotFoundException {
        UserEntity usuario = usuarioRepository.findBySAMAccountNameWithRoles(UserEntity) // <-- Usar este método
                .orElseThrow(() -> new UsernameNotFoundException("Usuario no encontrado"));

        // Extraer permisos desde los roles del usuario
        List<GrantedAuthority> authorities = usuario.getRoles().stream()
                .flatMap(rol -> rol.getPermisos().stream()) // Obtener permisos de cada rol
                .map(permiso -> new SimpleGrantedAuthority(permiso.getName_permission())) // Usar el nombre del permiso
                .collect(Collectors.toList());

        return new org.springframework.security.core.userdetails.User(
                usuario.getSAMAccountName(),
                usuario.getPassword(),
                authorities);
    }
}
