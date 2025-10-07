# Usar a imagem base do Python
FROM python:3.9-slim

# Instalar o git
RUN apt-get update && apt-get install -y git

# Definir diretório de trabalho
WORKDIR /app

# Copiar todos os arquivos do projeto para o contêiner
COPY . .

# Instalar dependências do Python
RUN pip install -r requirements.txt

# Comando para iniciar o app
CMD ["python", "app.py"]
