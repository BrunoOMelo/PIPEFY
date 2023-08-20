from __future__ import print_function
import os
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime



# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# The ID and range of a sample spreadsheet.
SAMPLE_SPREADSHEET_ID = '1UEzN1BuJFWys3Eg74V1wjQl5qwasuWTpoDjsnhcItOg'
SAMPLE_RANGE_NAME = 'base!A:O'


def main():

    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'client_secret.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    
    service = build('sheets', 'v4', credentials=creds)
    

    # # Call the Sheets API
    # sheet = service.spreadsheets()
    # result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
    #                                 range=SAMPLE_RANGE_NAME).execute()
    # values_online = result.get('values', [])

    # print('Valores anteriores ----> ')
    # print(values_online)
    


    import pandas as pd
    import requests
    import numpy as np
    import re 
    data_atual = datetime.now()
    data_formatada = data_atual.strftime("%Y-%m-%d")



    url = "https://api.pipefy.com/graphql"
    query = '''
    {
    allCards(pipeId: 301771911 last: 10 filter: {field: "updated_at", operator: gte, value: "DATE.T00:00:00-00:00",  AND: [{field: "updated_at", operator: lte, value: "DATE.T23:59:59-00:00"}]}
    ) {
        edges {
        node {
            id
            title
            
            fields {
            name
            value
            }
            phases_history {
            phase {
                name
            }
            firstTimeIn
            lastTimeOut
            }
        }
        }
        pageInfo {
        hasNextPage
        endCursor
        }
    }
    }
    '''
    query = query.replace('DATE.',data_formatada)
    payload = {"query": query}
    headers = {
    "accept": "application/json",
    "content-type": "application/json",
    "authorization": "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJ1c2VyIjp7ImlkIjozMDEzMzIyNzcsImVtYWlsIjoiZmluYW5jZWlyb0Bjcm51dHJpdGlvbi5jb20uYnIiLCJhcHBsaWNhdGlvbiI6MzAwMjMwMjIyfX0.fadwG2-bJWS23XlgqEGDE18el9cBybcQWF0vcMBFMk5EdBgtrPgTyLCLz-_yJtmFsEjAN8HCwGP30aypfDkcBA"
    }
    response = requests.post(url, json=payload, headers=headers)



    # %%
    df_pipefy = pd.json_normalize(response.json(),record_path=['data','allCards','edges']
                                #,meta=[['allCards','edges','node','id']],errors='ignore'
                                )



    # %%
    df_length = df_pipefy.shape
    print(df_length)



    # %%
    df_pipefy.drop(df_pipefy.columns[[1,2,3]], axis=1,inplace=True)
    df_pipefy

    # %%
    df_pipefy2 = pd.json_normalize(response.json(),record_path=['data','allCards','edges',['node','fields']]
                                ##,meta=[['allCards','edges','node','id']],errors='ignore'
                                )


    # %%
    # %%


    # DataFrame original
    df = df_pipefy2

    new_rows = []
    prev_row_has_anexo = False
    for index, row in df.iterrows():
        if row['name'] == 'Prioridade ' and not prev_row_has_anexo:
            new_rows.append(['Anexo', '[]'])
        new_rows.append(row.tolist())
        if row['name'] == 'Anexo':
            prev_row_has_anexo = True
        else:
            prev_row_has_anexo = False

    df_pipefy2 = pd.DataFrame(new_rows, columns=df.columns)

    # Remover linhas com valor 'Número de telefone' na coluna 'value'
    df_pipefy2 = df_pipefy2[df_pipefy2['name'] != 'Número de telefone']

    # Dividir o DataFrame em subsets de 10 linhas
    subsets = np.array_split(df_pipefy2, len(df_pipefy2) // 10)


    # Ordenar cada subconjunto do DataFrame
    categorias = ['Data', 'Email',  'Adicione a Prioridade', 'Empreendimento',
                'Nome do solicitante', 'Modulo do Sistema', 'Tipo de Chamado', 'Descreva a necessidade',
                'Anexo', 'Prioridade']

    for subset in subsets:
        subset.sort_values(by='name', key=lambda x: pd.Categorical(x, categories=categorias, ordered=True), inplace=True)

    # Concatenar verticalmente cada subconjunto do DataFrame
    df_sorted = pd.concat(subsets)




    # %%
    pivot2 = df_sorted.T

    # %%

    # dividindo o DataFrame em sub-DataFrames a cada 11 colunas
    sub_dfs = np.array_split(pivot2.values, len(pivot2.columns) // 10, axis=1)

    # transformando cada sub-DataFrame em um DataFrame e juntando em um só
    df_pipefy3 = pd.concat([pd.DataFrame(sub) for sub in sub_dfs], axis=0, ignore_index=True)



    # transformando a primeira linha em cabeçalho
    new_header = df_pipefy3.iloc[0]
    df_pipefy3 = df_pipefy3[1:]
    df_pipefy3.columns = new_header

    # excluindo as linhas ímpares
    df_pipefy3 = df_pipefy3.iloc[::2]



    # %%
    merge1 = df_pipefy3

    # %%
    merge1.reset_index(drop=True, inplace=True)
    merge1.head()

    # %%
    merge = pd.merge(df_pipefy,merge1, right_index=True, left_index=True)



    # %%
    def remover_marcações(texto):
        return re.sub(r'\W+', ' ', texto)

    merge['Descreva a necessidade']= merge['Descreva a necessidade'].apply(remover_marcações)

    # %%
    colunas_limpeza = [3, 7, 9,10]

    # loop para aplicar a limpeza em todas as colunas da lista
    for coluna in colunas_limpeza:
        merge.iloc[:, coluna] = merge.iloc[:, coluna].apply(lambda x: str(x).
                                                            replace('[','').replace(']','').replace('"',''))

    # %%
    merge = merge.drop(merge.columns[[1,2,8,9]], axis=1)



    # %%
    merge = merge[['node.id','Nome do solicitante','Prioridade ','Empreendimento','Modulo do Sistema','Tipo de Chamado']]







    # %%

    df_pipefy3 = pd.json_normalize(response.json(),record_path=['data','allCards','edges',['node','phases_history']])

    # %%
    df = df_pipefy3



    # separar em dois dataframes
    df_firstTimeIn = df[['firstTimeIn', 'phase.name']].rename(columns={'firstTimeIn': 'datetime'})
    df_lastTimeOut = df[['lastTimeOut', 'phase.name']].rename(columns={'lastTimeOut': 'datetime'})

    # adicionar sufixo '- end' em df_lastTimeOut['phase.name']
    df_lastTimeOut['phase.name'] = df_lastTimeOut['phase.name'] + ' - end'

    # converter para datetime
    df_firstTimeIn['datetime'] = pd.to_datetime(df_firstTimeIn['datetime'])
    df_lastTimeOut['datetime'] = pd.to_datetime(df_lastTimeOut['datetime'])


    # %%
    df_lastTimeOut = df_lastTimeOut.set_index('phase.name')
    df_firstTimeIn = df_firstTimeIn.set_index('phase.name')

    # %%
    df_lastTimeOut = df_lastTimeOut.T
    df_firstTimeIn = df_firstTimeIn.T



    # %%
    df_lastTimeOut.reset_index(drop=True, inplace=True)
    df_firstTimeIn.reset_index(drop=True, inplace=True)

    # %%
    df_firstTimeIn = df_firstTimeIn.T
    df_firstTimeIn.reset_index(inplace=True)
    df_lastTimeOut = df_lastTimeOut.T
    df_lastTimeOut.reset_index(inplace=True)



    # %%

    # Função para verificar e adicionar linhas ausentes na sequência
    def verificar_sequencia_in(subconjunto):
        sequencia_esperada = ['Start form', 'Inicio', 'Em atendimento', 'Concluído']
        coluna1 = subconjunto['phase.name'].tolist()

        if coluna1[:len(sequencia_esperada)] != sequencia_esperada:
            for valor in sequencia_esperada:
                if valor not in coluna1:
                    nova_linha = pd.DataFrame([[valor, np.nan]], columns=subconjunto.columns)
                    subconjunto = pd.concat([subconjunto, nova_linha], ignore_index=True)

        return subconjunto



    df = df_firstTimeIn

    # Dividindo o DataFrame em subconjunto_end com base em 'Start form'
    indices = df[df['phase.name'] == 'Start form'].index.tolist()
    indices.append(len(df))




    subconjunto_in = []
    start_index = 0

    for end_index in indices:
        subconjunto = df.iloc[start_index:end_index]
        subconjunto_in.append(subconjunto)
        start_index = end_index

    # Verificando e adicionando linhas ausentes na sequência em cada subconjunto
    subconjunto_in = [verificar_sequencia_in(subconjunto) for subconjunto in subconjunto_in]

    # Transpor e concatenar horizontalmente os DataFrames
    df_concatenado = pd.concat([subconjunto.T for subconjunto in subconjunto_in], axis=1)



    # Definir a próxima linha como o novo cabeçalho
    new_header = df_concatenado.iloc[0]  # Extrair a próxima linha como o novo cabeçalho
    df_concatenado = df_concatenado[1:]  # Excluir a próxima linha do DataFrame
    df_concatenado.columns = new_header  # Definir o novo cabeçalho




    # Resetar o índice do DataFrame
    df_concatenado = df_concatenado.reset_index(drop=True)
    # Descartar as primeiras 4 colunas
    df_concatenado = df_concatenado.iloc[:, 4:]



    # %%
    # Função para verificar e adicionar linhas ausentes na sequência
    def verificar_sequencia_end(subconjunto):
        sequencia_esperada = ['Start form - end', 'Inicio - end', 'Em atendimento - end', 'Concluído - end']
        coluna1 = subconjunto['phase.name'].tolist()

        if coluna1[:len(sequencia_esperada)] != sequencia_esperada:
            for valor in sequencia_esperada:
                if valor not in coluna1:
                    nova_linha = pd.DataFrame([[valor, np.nan]], columns=subconjunto.columns)
                    subconjunto = pd.concat([subconjunto, nova_linha], ignore_index=True)

        return subconjunto




    df = df_lastTimeOut

    # Dividindo o DataFrame em subconjunto_end com base em 'Start form'
    indices = df[df['phase.name'] == 'Start form - end'].index.tolist()
    indices.append(len(df))

    subconjunto_end = []
    start_index = 0

    for end_index in indices:
        subconjunto = df.iloc[start_index:end_index]
        subconjunto_end.append(subconjunto)
        start_index = end_index

    # Verificando e adicionando linhas ausentes na sequência em cada subconjunto
    subconjunto_end = [verificar_sequencia_end(subconjunto) for subconjunto in subconjunto_end]

    df_concatenado_end = pd.concat([subconjunto.T for subconjunto in subconjunto_end], axis=1)



    # Definir a próxima linha como o novo cabeçalho
    new_header = df_concatenado_end.iloc[0]  # Extrair a próxima linha como o novo cabeçalho
    df_concatenado_end = df_concatenado_end[1:]  # Excluir a próxima linha do DataFrame
    df_concatenado_end.columns = new_header  # Definir o novo cabeçalho

    # Resetar o índice do DataFrame
    df_concatenado_end = df_concatenado_end.reset_index(drop=True)



    # Descartar as primeiras 4 colunas
    df_concatenado_end = df_concatenado_end.iloc[:, 4:]
    df_concatenado_end.columns.name = None


  
        
    # %%
    # Divide o DataFrame original em vários sub-DataFrames de 4 colunas cada
    sub_dfsend = [df_concatenado_end.iloc[:, i:i+4] for i in range(0, df_concatenado_end.shape[1], 4)]
    df_end = pd.concat(sub_dfsend, axis=0).reset_index(drop=True)
    sub_dfsin = [df_concatenado.iloc[:, i:i+4] for i in range(0, df_concatenado.shape[1], 4)]
    df_in = pd.concat(sub_dfsin, axis=0).reset_index(drop=True)
    



    

   


    # Renomeia as colunas de cada sub-DataFrame com os nomes das 4 primeiras colunas originais
    for i, sub_df in enumerate(sub_dfsend):
        sub_df.columns = df_concatenado_end.columns[i*4:i*4+4]
        sub_df.columns = df_concatenado.columns[i*4:i*4+4]

      
 
    # Concatena todos os sub-DataFrames em um único DataFrame com as mesmas 4 colunas originais
    df_end = pd.concat(sub_dfsend, axis=0)    
    df_in = pd.concat(sub_dfsin, axis=0)    

 


    df_in.drop(['Start form'], axis=1, inplace=True)
    df_end.drop(['Start form'], axis=1, inplace=True)
    
    df_in.reset_index(drop=True,inplace=True)
    df_end.reset_index(drop=True,inplace=True)
    
    df_end.rename(columns={'Inicio': 'Inicio - end', 'Em atendimento':'Em atendimento - end', 'Concluído':'Concluído - end'}, inplace=True)
    
    
    # %%
    
    
    df_phases = pd.concat([df_in, df_end], axis=1)

    
    # %%
    df_final = pd.concat([merge, df_phases], axis=1)
    

    
    # %%
    df_final = df_final.drop_duplicates(keep='last')
    df_final.reset_index(drop=True,inplace=True)
    
    
    
    # %%
    columns_to_convert = ['Inicio', 'Inicio - end', 'Em atendimento', 'Em atendimento - end', 'Concluído', 'Concluído - end']
    existing_columns = df_final.columns.intersection(columns_to_convert)
    df_final[existing_columns] = df_final[existing_columns].astype(str)
    
    
    
    # %%
    df_final.replace('NaT',np.nan,regex=True, inplace=True)

    
    # %%
    
    
    # Função personalizada para encontrar a última coluna não nula em cada linha
    def encontrar_ultima_coluna(row):

        ultima_coluna = None
        for col in df_final.columns[::-1]:
            if (pd.notna(row[col])):
                ultima_coluna = col
                break
        return ultima_coluna
    
    # Aplicar a função personalizada a cada linha do DataFrame e adicionar uma nova coluna com o resultado
    df_final['Fase atual'] = df_final.apply(encontrar_ultima_coluna, axis=1)
    

    
    
    # %%
    df_final['Fase atual'] = df_final['Fase atual'].replace('Em atendimento - end', 'Concluído')
    df_final['Fase atual'] = df_final['Fase atual'].replace('Inicio - end', 'Em atendimento')
    df_final['Fase atual'] = df_final['Fase atual'].replace('Tipo de Chamado', 'Concluído')
 #   df_final['Fase atual'] = df_final['Fase atual'].replace('Concluído - end', 'Concluído')
    
    
    
    # %%
    df_final['Solicitante - Empreendimento'] = df_final['Nome do solicitante'].astype(str).str.cat(df_final['Empreendimento'], sep=' - ')
    
    # %%
    columns_to_include = ['node.id', 'Solicitante - Empreendimento', 'Fase atual', 'Prioridade ', 'Empreendimento',  'Modulo do Sistema', 'Tipo de Chamado', 'Inicio', 'Inicio - end', 'Em atendimento', 'Em atendimento - end']
    existing_columns = [col for col in columns_to_include if col in df_final.columns]
    df_final = df_final.reindex(columns=existing_columns)
    
    
    # %%
    if 'Inicio' in df_final.columns:
        df_final['Inicio'] = pd.to_datetime(df_final['Inicio'], errors='coerce')
    
    if 'Inicio - end' in df_final.columns:
        df_final['Inicio - end'] = pd.to_datetime(df_final['Inicio - end'], errors='coerce')
    
    if 'Em atendimento' in df_final.columns:
        df_final['Em atendimento'] = pd.to_datetime(df_final['Em atendimento'], errors='coerce')
    
    if 'Em atendimento - end' in df_final.columns:
        df_final['Em atendimento - end'] = pd.to_datetime(df_final['Em atendimento - end'], errors='coerce')
    
    
    
    
    # %%

    
    
    # %%
    try:
        # Calculando a diferença entre as colunas 'Inicio - end' e 'Inicio'
        df_final['tempo_inicio'] = (df_final['Inicio - end'] - df_final['Inicio'])
    except Exception as e:
        df_final['tempo_inicio'] = pd.NaT
    
    
    try:
    
        df_final['tempo_atendimento'] = (df_final['Em atendimento - end'].fillna(0) - df_final['Em atendimento'].fillna(0))
    
    except Exception as e:
        df_final['tempo_atendimento'] = pd.NaT
    

    
    # %%
    try:
        list_atendimento = []
        list_inicio = []
        for i in range(len(df_final.index)):
            s = df_final['tempo_atendimento'][i].total_seconds()
            s2 = df_final['tempo_inicio'][i].total_seconds()
            hours = s // 3600
            hours2 = s2 // 3600 
            s = s - (hours * 3600)
            s2 = s2 - (hours2 * 3600)
            minutes = s // 60
            minutes2 = s2 // 60
            seconds = s - (minutes * 60)
            seconds2 = s2 - (minutes2 * 60)
            list_atendimento.append(('{:02}:{:02}:{:02}'.format(int(hours), int(minutes), int(seconds))))
            list_inicio.append(('{:02}:{:02}:{:02}'.format(int(hours2), int(minutes2), int(seconds2))))
    
        df_final['tempo_atendimento_continuo'] = list_atendimento
        df_final['tempo_inicio_continuo'] = list_inicio
    except:
        df_final['tempo_atendimento_continuo'] = ''
        df_final['tempo_inicio_continuo'] = ''
    
    
    # %%
    columns_to_convert = ['Inicio', 'Inicio - end', 'Em atendimento', 'Em atendimento - end',
                      'tempo_atendimento', 'tempo_inicio', 'tempo_atendimento_continuo',
                      'tempo_inicio_continuo']

    for column in columns_to_convert:
        if column in df_final.columns:
            df_final[column] = df_final[column].astype(str)

        values = df_final.values.tolist()
    
    
    resource = {
        "majorDimension": "ROWS",
        "values": values
        }
    spreadsheetId = SAMPLE_SPREADSHEET_ID
    range_datas = SAMPLE_RANGE_NAME;
    service.spreadsheets().values().append(
        spreadsheetId=spreadsheetId,
        range=range_datas,
        body=resource,
        valueInputOption="USER_ENTERED"
        ).execute()
    
    
    
    print('Execução: ')
    print(datetime.now())
    


if __name__ == '__main__':
    import time

    while True:
        try:
           
            main()
            time.sleep(60)
        except:
            print('Falha')
            print(datetime.now())
            time.sleep(60)
            continue
        
    
    
    
    

# %%


# %%


# %%


# %%



