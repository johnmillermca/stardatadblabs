FROM python:3.11-slim
WORKDIR /app
COPY polaris_auth_proxy.py /app/polaris_auth_proxy.py
# Pure stdlib — no pip deps
EXPOSE 8282 8283
CMD ["python", "/app/polaris_auth_proxy.py"]
