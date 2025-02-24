package com.cgr.base.infrastructure.externar.repository;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.cgr.base.domain.repository.IActiveDirectoryUserRepository;
import com.cgr.base.infrastructure.persistence.entity.user.UserEntity;
import com.unboundid.ldap.sdk.LDAPBindException;
import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.SearchRequest;
import com.unboundid.ldap.sdk.SearchResult;
import com.unboundid.ldap.sdk.SearchResultEntry;
import com.unboundid.ldap.sdk.SearchScope;

@Component
public class LDAPUsuarioRepository implements IActiveDirectoryUserRepository {

    @Value("${LDAP_HOST}")
    private String ldapHost;

    @Value("${LDAP_PORT}")
    private int ldapPort;

    @Value("${LDAP_BASE_DN}")
    private String baseDN;

    @Value("${LDAP_DOMAIN}")
    private String domain;

    @Value("${LDAP_SERVICE_USER}")
    private String serviceUser;

    @Value("${LDAP_SERVICE_PASSWORD}")
    private String servicePassword;

    // Verificar credenciales en Active Directory
    @Override
    public Boolean checkAccount(String samAccountName, String password) {
        try (LDAPConnection connection = new LDAPConnection(ldapHost, ldapPort)) {
            String userPrincipalName = samAccountName + "@" + domain;
            connection.bind(userPrincipalName, password);

            SearchResultEntry usuario = buscarUsuarioPorSAMAccountName(connection, samAccountName, baseDN);
            if (usuario == null) {
                throw new IllegalArgumentException("User not found in Active Directory: " + samAccountName);
            }

            return true;

        } catch (LDAPBindException e) {
            throw new IllegalArgumentException("Invalid credentials for user: " + samAccountName, e);
        } catch (LDAPException e) {
            throw new IllegalStateException("Error connecting to LDAP server.", e);
        }
    }

    // Buscar usuario por SAMAccountName en el directorio LDAP.
    private SearchResultEntry buscarUsuarioPorSAMAccountName(
            LDAPConnection connection, String samAccountName, String baseDN) throws LDAPException {

        String searchFilter = String.format("(sAMAccountName=%s)", samAccountName);
        SearchRequest searchRequest = new SearchRequest(baseDN, SearchScope.SUB, searchFilter);

        SearchResult searchResult = connection.search(searchRequest);

        if (searchResult.getEntryCount() == 0) {
            return null;
        }
        return searchResult.getSearchEntries().get(0);
    }

    // Obtener todos los usuarios del Active Directory
    @Override
    public List<UserEntity> getAllUsers() {
        List<UserEntity> users = new ArrayList<>();

        try (LDAPConnection connection = new LDAPConnection(ldapHost, ldapPort)) {
            connection.bind(serviceUser, servicePassword);

            String searchFilter = "(objectClass=user)";
            SearchRequest searchRequest = new SearchRequest(baseDN, SearchScope.SUB, searchFilter);

            SearchResult searchResult = connection.search(searchRequest);

            for (SearchResultEntry entry : searchResult.getSearchEntries()) {
                UserEntity userEntity = new UserEntity();

                userEntity.setSAMAccountName(entry.getAttributeValue("sAMAccountName"));
                userEntity.setFullName(entry.getAttributeValue("displayName"));
                userEntity.setEmail(entry.getAttributeValue("userPrincipalName"));
                userEntity.setEnabled(this.isEnabledUser(entry.getAttributeValue("userAccountControl")));
                userEntity.setPhone(entry.getAttributeValue("mobile"));
                userEntity.setCargo(entry.getAttributeValue("Title"));

                String whenChanged = entry.getAttributeValue("whenChanged");
                if (whenChanged != null) {
                    Date formattedDate = formatWhenChanged(whenChanged);
                    userEntity.setDateModify(formattedDate);
                }

                users.add(userEntity);
            }

        } catch (LDAPException e) {
            throw new IllegalStateException("Error retrieving users from Active Directory.", e);
        }

        if (users.isEmpty()) {
            throw new IllegalStateException("No users found in Active Directory.");
        }

        return users;
    }

    private Boolean isEnabledUser(String userAccountControl) {
        if (userAccountControl != null) {
            try {
                int uacValue = Integer.parseInt(userAccountControl);
                boolean isDisabled = (uacValue & 0x2) != 0;
                return !isDisabled;
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid userAccountControl value: " + userAccountControl, e);
            }
        } else {
            throw new IllegalArgumentException("Missing userAccountControl attribute.");
        }
    }

    private Date formatWhenChanged(String whenChanged) {
        try {
            SimpleDateFormat adFormat = new SimpleDateFormat("yyyyMMddHHmmss");
            if (whenChanged.contains(".")) {
                whenChanged = whenChanged.split("\\.")[0];
            }
            return adFormat.parse(whenChanged);

        } catch (ParseException e) {
            throw new IllegalArgumentException("Error parsing 'whenChanged' attribute: " + whenChanged, e);
        }
    }
}
