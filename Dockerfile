FROM nginx:1.25-alpine

# Copiar los archivos de Nginx
COPY nginx.conf /etc/nginx/conf.d/

# Copiar los certificados SSL (ubicados en la carpeta nginx de la raíz del proyecto)
COPY ssl/mycertificate.pem /etc/ssl/certs/mycertificate.pem
COPY ssl/myprivatekey.pem /etc/ssl/private/myprivatekey.pem

# Eliminar configuración por defecto y deshabilitar scripts de entrada
RUN rm -rf /etc/nginx/conf.d/default.conf && \
    rm -rf /docker-entrypoint.d/*

# Asignar permisos a los archivos estáticos
RUN chown -R nginx:nginx /usr/share/nginx/html && \
    chmod -R 755 /usr/share/nginx/html

# Exponer puertos 80 y 443
EXPOSE 80 443

CMD ["nginx", "-g", "daemon off;"]
