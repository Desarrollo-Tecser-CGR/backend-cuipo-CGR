package com.cgr.base.external.LDAP;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.cgr.base.entity.user.UserEntity;
import com.cgr.base.external.CGR.ExternalAuthService;
import com.cgr.base.repository.auth.IActiveDirectoryUserRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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

    @Autowired
    ExternalAuthService externalAuthService;

    @Value("${external.auth.mode}")
    private String authMode;

    private final RestTemplate restTemplate = new RestTemplate();

    private final String externalUserInfoUrl = "https://serviciosint.contraloria.gov.co/directorio/usuarios/consultar/";

    // Verificar credenciales en Active Directory
    @Override
    public Boolean checkAccount(String samAccountName, String password) {

        if ("ldap".equalsIgnoreCase(authMode)) {
            return authenticateWithLDAP(samAccountName, password);
        } else if ("cgr".equalsIgnoreCase(authMode)) {
            return authenticateWithExternalCGR(samAccountName, password);
        } else {
            throw new IllegalStateException("Unknown authentication mode: " + authMode);
        }
    }

    private boolean authenticateWithLDAP(String samAccountName, String password) {
        try (LDAPConnection connection = new LDAPConnection(ldapHost, ldapPort)) {
            String userPrincipalName = samAccountName + "@" + domain;
            connection.bind(userPrincipalName, password);

            SearchResultEntry usuario = getUserDirectoryBySAMAccountName(connection, samAccountName, baseDN);
            if (usuario == null) {
                return false;
            }

            return true;

        } catch (LDAPBindException e) {
            return false;
        } catch (LDAPException e) {
            throw new SecurityException("Error connecting to LDAP server.", e);
        }
    }

    private boolean authenticateWithExternalCGR(String samAccountName, String password) {
        try {
            boolean isAuthenticated = externalAuthService.authenticateWithExternalService(samAccountName, password);
            if (!isAuthenticated) {
                return false;
            }

            UserEntity userEntity = getUserDirectoryCGR(samAccountName);
            if (userEntity == null) {
                return false;
            }

            return true;

        } catch (Exception e) {
            throw new SecurityException("Error connecting to external CGR service.", e);
        }
    }

    public UserEntity getUserDirectoryCGR(String samAccountName) {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.set("usuario", samAccountName);

            ResponseEntity<String> response = restTemplate.getForEntity(externalUserInfoUrl + samAccountName,
                    String.class);

            if (response.getStatusCode().is2xxSuccessful()) {

                ObjectMapper objectMapper = new ObjectMapper();
                JsonNode jsonNode = objectMapper.readTree(response.getBody());

                UserEntity userEntity = new UserEntity();
                userEntity.setSAMAccountName(jsonNode.get("usuario").asText());
                userEntity.setFullName(jsonNode.get("nombreMostrar").asText());

                String correoElectronico = jsonNode.path("correoElectronico").asText(null);
                if (correoElectronico != null) {
                    userEntity.setEmail(correoElectronico);
                }

                String numeroTelefono = jsonNode.path("numeroTelefono").asText(null);
                if (numeroTelefono != null) {
                    userEntity.setPhone(numeroTelefono);
                }

                String descripcionCargo = jsonNode.path("descripcionCargo").asText(null);
                if (descripcionCargo != null) {
                    userEntity.setCargo(descripcionCargo);
                }

                userEntity.setEnabled(true);

                return userEntity;
            } else {
                System.err.println("Error al obtener información del usuario desde el endpoint de validacion.");
                return null;
            }
        } catch (Exception e) {
            return null;
        }
    }

    // Buscar usuario por SAMAccountName en el directorio LDAP.
    private SearchResultEntry getUserDirectoryBySAMAccountName(
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
