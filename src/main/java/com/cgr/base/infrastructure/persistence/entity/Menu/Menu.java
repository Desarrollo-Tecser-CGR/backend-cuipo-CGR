package com.cgr.base.infrastructure.persistence.entity.Menu;

import java.util.HashSet;
import java.util.Set;

import com.cgr.base.infrastructure.persistence.entity.role.RoleEntity;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonManagedReference;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.JoinTable;
import jakarta.persistence.ManyToMany;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;
import lombok.Data;

@Entity
@Data
@Table(name = "menus")
public class Menu {


    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String title;

    @Column(nullable = false)
    private String subtitle;

    @Column(nullable = false)
    private String type;

    @Column(nullable = false)
    private String icon;

    // Relación 1:N con SubMenuEntity
    @OneToMany(mappedBy = "menu", cascade = CascadeType.ALL, orphanRemoval = true)
    @JsonManagedReference
    private Set<SubMenuEntity> children = new HashSet<>();

    // ManyToMany con roles (tabla intermedia menu_roles)
    @ManyToMany
    @JoinTable(
        name = "menu_roles",
        joinColumns = @JoinColumn(name = "menu_id"),
        inverseJoinColumns = @JoinColumn(name = "role_id")
    )
    @JsonIgnore
    private Set<RoleEntity> roles = new HashSet<>();

    public Menu() {
    }

    public Menu(Long id,
                String title,
                String subtitle,
                String type,
                String icon,
                Set<SubMenuEntity> children) {
        this.id = id;
        this.title = title;
        this.subtitle = subtitle;
        this.type = type;
        this.icon = icon;
        this.children = children;
    }

    @Override
    public String toString() {
        return "Menu [id=" + id
                + ", title=" + title
                + ", subtitle=" + subtitle
                + ", type=" + type
                + ", icon=" + icon
                + ", children=" + children
                + ", roles=" + (roles != null ? roles.size() : 0)
                + "]";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null || getClass() != obj.getClass())
            return false;
        Menu other = (Menu) obj;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        if (children == null) {
            if (other.children != null)
                return false;
        } else if (!children.equals(other.children))
            return false;
        // Si quieres comparar roles, hazlo aquí (aunque a veces puede causar
        // recursión).
        // if (roles == null) {
        // if (other.roles != null)
        // return false;
        // } else if (!roles.equals(other.roles))
        // return false;
        return true;
    }

    @Override
    public int hashCode() {
        // Genera tu hashCode, típicamente en base a 'id'.
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((children == null) ? 0 : children.hashCode());
        // Si quisieras incluir roles, hazlo aquí.
        return result;
    }

}
