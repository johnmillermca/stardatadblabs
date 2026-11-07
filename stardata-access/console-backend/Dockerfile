FROM 192.168.1.50:30500/busybox:1.36
RUN adduser -D -u 1000 appuser 2>/dev/null || true
COPY console-backend-bin /usr/local/bin/console-backend
RUN chmod +x /usr/local/bin/console-backend
USER 1000
EXPOSE 8090
ENTRYPOINT ["/usr/local/bin/console-backend"]
