import psycopg2

def verificar_novos_dados():
    conn = psycopg2.connect(
        host="seu_host",
        database="seu_banco",
        user="seu_usuario",
        password="sua_senha"
    )
    cursor = conn.cursor()

    # Exemplo de consulta para detectar novos dados com base em um campo de timestamp
    cursor.execute("SELECT * FROM tabela_origem WHERE timestamp > (SELECT max(timestamp) FROM tabela_destino)")
    novos_dados = cursor.fetchall()

    conn.close()
    return novos_dados