from data_process import Data

path_empresaa = 'data_raw/dados_empresaA.json'
path_empresab = 'data_raw/dados_empresaB.csv' 


##Extract
data_empresaA = Data(path_empresaa, 'json')
data_empresaB = Data(path_empresab, 'csv')

#Data Transformation
key_mapping = {
    'Nome do Item':'Nome do Produto', 
    'ClassificaÃ§Ã£o do Produto':'Categoria do Produto', 
    'Valor em Reais (R$)':'Preço do Produto (R$)', 
    'Quantidade em Estoque':'Quantidade em Estoque', 
    'Nome da Loja':'Filial',
    'Data da Venda': 'Data da Venda'
}
data_empresaB.rename_columns(key_mapping)
print(data_empresaB.data[0])
print(data_empresaA.data[0])
joined_data = Data.join(data_empresaA, data_empresaB)

#Saving
joined_data_path = 'data_processed/combined_data.csv'
joined_data.saving_data(joined_data_path)