import pandas as pd 
import os           

print("--- ETAPA DE EXTRAÇÃO ---")
nome_arquivo_entrada = 'vendas_diarias.csv'

if not os.path.exists(nome_arquivo_entrada):
    print(f"Erro: O arquivo '{nome_arquivo_entrada}' não foi encontrado.")
    print("Por favor, crie o arquivo na mesma pasta do script com o conteúdo fornecido.")
    exit()

try:
    df_vendas_brutas = pd.read_csv(nome_arquivo_entrada)
    print(f"Dados brutos extraídos de '{nome_arquivo_entrada}':")
    print(df_vendas_brutas)
    print("\n")
except Exception as e:
    print(f"Erro ao extrair dados do CSV: {e}")
    exit()

print("--- ETAPA DE TRANSFORMAÇÃO ---")

df_vendas_transformadas = df_vendas_brutas.copy()

df_vendas_transformadas['valor_total_item'] = df_vendas_transformadas['quantidade'] * df_vendas_transformadas['preco_unitario']
print("DataFrame após calcular 'valor_total_item':")
print(df_vendas_transformadas)
print("\n")

def categorizar_produto(produto_nome):
    produto_nome_lower = produto_nome.lower() 
    if 'leite' in produto_nome_lower or 'queijo' in produto_nome_lower:
        return 'Laticínios'
    elif 'pao' in produto_nome_lower or 'biscoito' in produto_nome_lower or 'chocolate' in produto_nome_lower:
        return 'Padaria/Doces'
    elif 'refrigerante' in produto_nome_lower or 'agua' in produto_nome_lower or 'cafe' in produto_nome_lower:
        return 'Bebidas'
    else:
        return 'Outros'

df_vendas_transformadas['categoria'] = df_vendas_transformadas['produto'].apply(categorizar_produto)
print("DataFrame após categorizar produtos:")
print(df_vendas_transformadas)
print("\n")

total_vendas_dia = df_vendas_transformadas['valor_total_item'].sum()
print(f"Total de vendas do dia: R$ {total_vendas_dia:.2f}")
print("\n")

vendas_por_categoria = df_vendas_transformadas.groupby('categoria')['valor_total_item'].sum().reset_index()
print("Vendas totais por categoria:")
print(vendas_por_categoria)
print("\n")

print("--- ETAPA DE CARREGAMENTO ---")

nome_arquivo_saida_detalhes = 'vendas_diarias_processadas_detalhes.csv'
nome_arquivo_saida_resumo = 'vendas_diarias_processadas_resumo.csv'

try:
    df_vendas_transformadas.to_csv(nome_arquivo_saida_detalhes, index=False)
    print(f"Dados detalhados carregados para '{nome_arquivo_saida_detalhes}'.")

    vendas_por_categoria.to_csv(nome_arquivo_saida_resumo, index=False)
    print(f"Resumo por categoria carregado para '{nome_arquivo_saida_resumo}'.")

    print("\n--- PROCESSO ETL CONCLUÍDO COM SUCESSO! ---")
    print(f"Você pode verificar os novos arquivos: '{nome_arquivo_saida_detalhes}' e '{nome_arquivo_saida_resumo}' na pasta do seu projeto.")

except Exception as e:
    print(f"Erro ao carregar os dados para os arquivos CSV: {e}")

