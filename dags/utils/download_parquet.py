import requests
import os

def download_parquet_from_url(url, destination_file_name=None):
    response = requests.get("https://storage.googleapis.com/desafio-eng-dados/2024-03-06.pq", stream=True)

    if response.status_code == 200:
        # Extrair o nome do arquivo do URL
        filename = url.split("/")[-1] # Obter a última parte do caminho do URL

        # Garantir que o nome do arquivo de destino seja fornecido
        if not destination_file_name:
            destination_file_name = "2024-03-06.pq" # Usar o nome do arquivo se não for fornecido

        # Salvar no diretório local (Windows)
        temp_path = os.path.join("D:\\temp", destination_file_name)

        # Criar diretório se ele não existir
        os.makedirs(os.path.dirname(temp_path), exist_ok=True)

        # Baixar em partes (chunks)
        with open(temp_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)

                print(f"Arquivo baixado com sucesso para {temp_path}")
        
        return temp_path
 
    else:
        print(f"Falha ao baixar o arquivo. Status code: {response.status_code}")
        return None

# Adicionando a mensagem de que o arquivo foi baixado
    downloaded_file_path = download_file(url)  # Supondo que esta seja a função que realiza o download do arquivo

    if downloaded_file_path:
        print(f"Arquivo baixado disponível em: {downloaded_file_path}")
    else:
        print("Falha no download.")