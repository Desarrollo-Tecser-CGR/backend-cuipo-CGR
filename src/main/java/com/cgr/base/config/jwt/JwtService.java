package com.cgr.base.config.jwt;

import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.JWTVerificationException;
import com.auth0.jwt.exceptions.TokenExpiredException;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.auth0.jwt.interfaces.JWTVerifier;
import com.cgr.base.dto.auth.AuthResponseDto;
import com.cgr.base.entity.role.RoleEntity;
import com.cgr.base.entity.user.UserEntity;
import com.cgr.base.repository.user.IUserRepositoryJpa;
import com.fasterxml.jackson.core.JsonProcessingException;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class JwtService {

    @Autowired
    private IUserRepositoryJpa userRepo;

    @Value("${jwt.secret.key}")
    private String secretKey;

    public String createToken(AuthResponseDto customerJwt, List<RoleEntity> roles) {

        Algorithm algorithm = Algorithm.HMAC256(secretKey);

        Date now = new Date();
        Date validity = new Date(now.getTime() + 3600000 * 2);

        String tokenCreated = JWT.create()
                .withClaim("userName", customerJwt.getSAMAccountName())      
                .withClaim("isEnabled", customerJwt.getIsEnable())
                .withIssuedAt(now)
                .withExpiresAt(validity)
                .sign(algorithm);

        return tokenCreated;
    }

    public String getClaimUserName(String token) {
        return JWT.decode(token).getClaim("userName").asString();
    }

    public boolean validateToken(String token) throws JsonProcessingException {

        if (isTokenExpired(token) != null)
            return true;

        if (!getUserDto(token).getIsEnable())
            return true;

        return false;

    }

    public AuthResponseDto getUserDto(String token) throws JsonProcessingException {

        String email = JWT.decode(token).getClaim("userName").asString();
        boolean isEnabled = JWT.decode(token).getClaim("isEnabled").asBoolean();

        return AuthResponseDto.builder()
                .sAMAccountName(email)
                .isEnable(isEnabled)
                .build();
    }

    public List<String> getRolesToken(String token) {
        return getDecodedJWT(token).getClaim("roles").asList(String.class);
    }

    public String validateFirma(String token) {
        try {

            Algorithm algorithm = Algorithm.HMAC256(secretKey);

            JWTVerifier verifier = JWT.require(algorithm)
                    .build();

            verifier.verify(token);

            return null;
        } catch (TokenExpiredException e) {
            return "The token has Expired.";
        } catch (JWTVerificationException e) {
            return "Invalid Token Signature or Corrupted Token.";
        } catch (Exception e) {
            return "Unexpected error Validating the Token: ".concat(e.getMessage());
        }
    }

    public String isTokenExpired(String token) {

        Date now = new Date();

        if (extractExpiration(token).before(now)) {
            Date expirationDate = extractExpiration(token);
            long timeUntilExpiration = expirationDate.getTime() - now.getTime();

            long hours = TimeUnit.MILLISECONDS.toHours(timeUntilExpiration);
            long minutes = TimeUnit.MILLISECONDS.toMinutes(timeUntilExpiration) % 60;
            long seconds = TimeUnit.MILLISECONDS.toSeconds(timeUntilExpiration) % 60;

            log.info(expirationDate.toString());

            return String.format("The token Expired in %d hours, %d minutes, and %d seconds at %s.", hours, minutes,
                    seconds,
                    expirationDate.toString());
        }

        return null;
    }

    private Date extractExpiration(String token) {

        return JWT.decode(token).getExpiresAt();
    }

    private DecodedJWT getDecodedJWT(String token) {
        Algorithm algorithm = Algorithm.HMAC256(secretKey);
        JWTVerifier verifier = JWT.require(algorithm).build();
        return verifier.verify(token);
    }

    public Long extractUserIdFromToken(String token) {
        
        String userName = JWT.decode(token).getClaim("userName").asString();
        Optional<UserEntity> user = userRepo.findBySAMAccountName(userName);
    
        if (!user.isPresent()) {
            return null;
        }
    
        Long userId = user.get().getId();
        return userId;
    }
    

}
