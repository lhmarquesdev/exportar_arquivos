# Usar a imagem base do Python
FROM python:3.9-slim

# Instalar dependências de sistema necessárias (como git)
RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*  # Limpar o cache do apt para reduzir o tamanho da imagem

# Definir diretório de trabalho
WORKDIR /app

# Copiar apenas o arquivo de dependências para otimizar o cache de construção
COPY requirements.txt .

# Instalar dependências do Python
RUN pip install --no-cache-dir -r requirements.txt

# Copiar o restante dos arquivos do projeto para o contêiner
COPY . .

# Definir a porta de exposição (se necessário)
EXPOSE 5000

# Comando para iniciar o app
CMD ["python", "app.py"]
