#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pymongo import MongoClient
import pandas as pd
import numpy as np


# ### Conexão com o banco de dados e leitura do documento

# In[3]:


uri = "mongodb+srv://teste_dados_leitura:o7c4Cc8NDeXYbAMH@mongodbtestebuilders.vuzqjs5.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(uri)
db = client['teste_dados']
collection = db['multas']
multas = collection.find()

client.close()


# ### Análise e transformando o documento em um DataFrame

# In[4]:


df = pd.DataFrame(list(multas))


# In[5]:


df


# In[6]:


df.info()


# In[7]:


df['quantidade_autos'].unique()


# In[8]:


df = df.astype({'quantidade_autos': float})


# In[9]:


df.info()


# In[10]:


df.describe()


# In[11]:


df[df["amparo_legal"].isna()]


# ### Criando dimensão escopo_autuacao

# In[12]:


escopo_autuacao=df['escopo_autuacao'].unique()


# In[13]:


escopo_autuacao


# In[14]:


dim_escopo_autuacao=pd.DataFrame(escopo_autuacao, index=range(1,4), columns=['escopo_autuacao'])


# In[15]:


dim_escopo_autuacao.reset_index().rename(columns={'index': 'escopo_autuacao_id'}
                                        ).to_csv('dim_escopo_autuacao.csv', index=False)


# #### Inserindo o escopo_autuacao_id para tabela que será fato

# In[16]:


condicoes = [(df['escopo_autuacao']=='Excesso de Peso'), 
              (df['escopo_autuacao']=='CMT - Capacidade Máxima de Tração'),
              (df['escopo_autuacao']=='Evasão de Balança')]

opcoes = [1,2,3]


# In[17]:


df['escopo_autuacao_id']= np.select(condicoes,opcoes)


# ### Criando dimensão UF

# In[18]:


uf= df['uf'].unique()


# In[19]:


uf


# In[20]:


dim_uf=pd.DataFrame(uf, index=range(1,13), columns=['uf'])


# In[21]:


dim_uf.reset_index().rename(columns={'index': 'uf_id'}).to_csv('dim_uf.csv', index=False)


# #### Inserindo o uf_id para tabela que será fato

# In[22]:


condicoes = [(df['uf']=='RJ'), 
             (df['uf']=='RS'),
             (df['uf']=='ES'),
             (df['uf']=='MG'),
             (df['uf']=='SC'),
             (df['uf']=='BA'),
             (df['uf']=='MS'),
             (df['uf']=='PR'),
             (df['uf']=='SP'),
             (df['uf']=='MT'),
             (df['uf']=='GO'),
             (df['uf']=='DF')
              ]

opcoes = [1,2,3,4,5,6,7,8,9,10,11,12]


# In[23]:


df['uf_id']= np.select(condicoes,opcoes)


# ### Criando dimensão Amparo legal

# In[24]:


df['amparo_legal'].fillna("Não possui amparo", inplace=True)


# In[25]:


amparo_legal = df['amparo_legal'].unique()


# In[26]:


amparo_legal


# In[27]:


dim_amparo_legal=pd.DataFrame(amparo_legal, index=range(1,6), columns=['amparo_legal'])


# In[28]:


dim_amparo_legal


# In[29]:


dim_amparo_legal.reset_index().rename(columns={'index': 'amparo_legal_id'}).to_csv('dim_amparo_legal.csv', index=False)


# #### Inserindo o amparo_legal_id para tabela que será fato
# 

# In[30]:


condicoes = [(df['amparo_legal']=='Lei 9503/97 Lei 10561/02 Res. CONTRAN nº 210/06, 211/06 e 258/07 e alterações Portaria DENATRAN 63/09 e 59/07 e alterações '), 
              (df['amparo_legal']=='Lei 9503 DE 23/09/1997 Transpor, sem autorização, bloqueio viário com ou sem sinalização ou dispositivos auxiliares, deixar de adentrar às áreas destinadas à pesagem de veículos ou evadir-se para não efetuar o pagamento do pedágio.'),
              (df['amparo_legal']=='Não possui amparo'),
              (df['amparo_legal']=='Lei 9503/97 Lei 10233/01 Res. CONTRAN nº 882/21 e alterações Portaria SENATRAN 354/22 e 268/22 e alterações '),
              (df['amparo_legal']=='Lei 9503/97 Lei 10233/01 Res. CONTRAN nº 882/21 e alterações Portaria SENATRAN 268/22 e alterações ')]

opcoes = [1,2,3,4,5]


# In[31]:


df['amparo_legal_id']= np.select(condicoes,opcoes)


# In[32]:


df


# In[33]:


df[df["amparo_legal"]=="Não possui amparo"]


# ### Criando dimensão Descricao_infracao

# In[34]:


descricao_infracao = df['descricao_infracao'].unique()


# In[35]:


descricao_infracao


# In[36]:


dim_descricao_infracao=pd.DataFrame(descricao_infracao, index=range(1,9), columns=['descricao_infracao'])


# In[37]:


dim_descricao_infracao.reset_index().rename(columns={'index': 'descricao_infracao_id'}
                                           ).to_csv('dim_descricao_infracao.csv', index=False)


# #### Inserindo o descricao_infracao_id para tabela que será fato

# In[38]:


condicoes = [(df['descricao_infracao']=='Transitar com o veículo com excesso de peso - PBT/PBTC e Por Eixo'), 
             (df['descricao_infracao']=='Transitar com o veículo com excesso de peso - PBT/PBTC'),
             (df['descricao_infracao']=='Transitar com o veículo com excesso de peso - Por Eixo'),
             (df['descricao_infracao']=='Transitar com o veículo excedendo a CMT acima de 1000 kg'),
             (df['descricao_infracao']=='Art. 209 Deixar de adentrar as áreas destinadas à pesagem de veículos'),
             (df['descricao_infracao']=='Transitar com o veículo excedendo a CMT em até 600 kg'),
             (df['descricao_infracao']=='Realizar transporte permissionado de passageiros, sem a emissão de bilhete.'),
             (df['descricao_infracao']=='Transitar com o veículo excedendo a CMT entre 601 e 1000 kg')
              ]

opcoes = [1,2,3,4,5,6,7,8]


# In[39]:


df['descricao_infracao_id']= np.select(condicoes,opcoes)


# In[40]:


df


# In[41]:


df['data'] =  df['mes'].map(str).str.lower() + '-' + df['ano'].map(str)


# In[42]:


df


# ### Tranformando as colunas de data em uma coluna no formato de data

# In[43]:


def ext_to_date(datas):
  data = datas.replace("-","/")
  meses = {'janeiro':'01', 'fevereiro':'02', r'março':'03', 'abril':'04', 
           'maio':'05', 'junho': '06', 'julho':'07', 'agosto': '08', 
           'setembro':'09', 'outubro':'10', 'novembro':'11', 'dezembro':'12'}
  for x in meses.keys():
    if x in datas:
      data=datas.replace(x,meses[x])
  return data


# In[44]:


a = []
for i in df.data:
  a.append(str(ext_to_date(i)))     


# In[45]:


df['data_dt'] = pd.to_datetime(a)


df


# In[46]:


df.info()


# ### Salvando tabela Fato

# In[47]:


fato_multas = df.drop(columns=['escopo_autuacao','mes','ano','uf','amparo_legal','descricao_infracao', 'data'])


# In[48]:


fato_multas


# In[49]:


fato_multas.describe()


# In[50]:


fato_multas.info()


# In[51]:


fato_multas.to_csv('fato_multas.csv', index=False)

