# Usar la imagen base con Java 17
FROM openjdk:17-jdk-slim

# Establecer el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copiar el archivo .jar generado al contenedor
COPY ./target/*.jar /app/app.jar

# Exponer el puerto 8001 (puerto de la aplicación Spring Boot)
EXPOSE 8001

# Comando para ejecutar la aplicación Spring Boot
ENTRYPOINT ["java", "-jar", "app.jar"]

