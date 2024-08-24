import os
import pandas as pd
import glob


# lista para rastrear os arquivos que já foram convertidos
def registrar_arquivo_processado(arquivo, arquivo_registro="arquivos_processados.txt"):
    with open(arquivo_registro, "a") as f:
        f.write(arquivo + "\n")


# percorrer automaticamente todos os arquivos Parquet na pasta especificada e verificar se já foram processados
def verificar_arquivo_processado(arquivo, arquivo_registro="arquivos_processados.txt"):
    if not os.path.exists(arquivo_registro):
        return False
    with open(arquivo_registro, "r") as f:
        arquivos_processados = f.read().splitlines()
    return arquivo in arquivos_processados


def manipular_parquet(pasta_origem="D:\\temp"):
    """
    Converte todos os arquivos Parquet encontrados na pasta especificada para CSV,
    ajustando os tipos das colunas e salvando os arquivos CSV na mesma pasta.

    Args:
        pasta_origem (str, optional): Caminho da pasta onde os arquivos Parquet estão localizados.
            Padrão: "D:\\temp".
    """

    # Encontra todos os arquivos Parquet na pasta
    arquivos_parquet = glob.glob(os.path.join(pasta_origem, "*.parquet"))

    for arquivo in arquivos_parquet:
        # Verifica se o arquivo já foi processado
        if verificar_arquivo_processado(arquivo):
            print(f"Arquivo já processado: {arquivo}")
            continue

        # Lê o arquivo Parquet
        df = pd.read_parquet(arquivo)

        # Ajuste dos tipos das colunas
        df["order_number"] = pd.to_numeric(df["order_number"], errors="coerce")
        df["terminal_id"] = pd.to_numeric(df["terminal_id"], errors="coerce")
        df["terminal_serial_number"] = df["terminal_serial_number"].astype(str)
        df["terminal_model"] = df["terminal_model"].astype(str)
        df["terminal_type"] = df["terminal_type"].astype(str)
        df["provider"] = df["provider"].astype(str)
        df["technician_email"] = df["technician_email"].astype(str)
        df["customer_phone"] = df["customer_phone"].astype(str)
        df["customer_id"] = df["customer_id"].astype(str)
        df["city"] = df["city"].astype(str)
        df["country"] = df["country"].astype(str)
        df["country_state"] = df["country_state"].astype(str)
        df["zip_code"] = pd.to_numeric(df["zip_code"], errors="coerce")
        df["street_name"] = df["street_name"].astype(str)
        df["neighborhood"] = df["neighborhood"].astype(str)
        df["complement"] = df["complement"].astype(str)
        df["arrival_date"] = pd.to_datetime(df["arrival_date"], errors="coerce")
        df["deadline_date"] = pd.to_datetime(df["deadline_date"], errors="coerce")
        df["cancellation_reason"] = df["cancellation_reason"].astype(str)
        df["last_modified_date"] = pd.to_datetime(
            df["last_modified_date"], errors="coerce"
        )

        # Obtém o nome do arquivo sem a extensão
        nome_arquivo, _ = os.path.splitext(arquivo)

        # Caminho para o arquivo CSV
        csv_output = nome_arquivo + ".csv"

        # Salva o DataFrame como CSV
        df.to_csv(csv_output, index=False)

        print(f"Arquivo CSV salvo em: {csv_output}")

        # Registra o arquivo como processado
        registrar_arquivo_processado(arquivo)


if __name__ == "__main__":
    manipular_parquet()