#!/usr/bin/env python
# coding: utf-8

# In[1]:


import mysql.connector
import pandas as pd
import numpy as np


# In[2]:


mydb = mysql.connector.connect(
  host="34.95.170.227",
  port="3306",
  user="teste-dados-leitura",
  passwd="o7c4Cc8NDeXYbAMH",
  database="teste_dados",
)

mydb.set_charset_collation('latin1', 'latin1_general_ci')


# In[3]:


query = "Select * from DADOS_COVID;"
dadosCovid = pd.read_sql(query,mydb)


# In[4]:


dadosCovid


# In[5]:


dadosCovid.describe()


# In[6]:


dadosCovid.info()


# ### Municípios brasileiros da base da receita federal para pegar com o nome correto

# ####
# https://www.gov.br/receitafederal/dados/municipios.csv

# In[7]:


cidadesBrasil = pd.read_csv('municipios.csv',encoding='latin1', delimiter=';')


# In[8]:


cidadesBrasil


# In[9]:


cidadesBrasil.info()


# ### Criando dimensão município

# In[10]:


cidadesBrasil = cidadesBrasil.drop(columns=['CÓDIGO DO MUNICÍPIO - TOM','MUNICÍPIO - IBGE', 'UF' ])


# In[11]:


cidadesBrasil


# In[12]:


cidadesBrasil = cidadesBrasil.rename(columns={'CÓDIGO DO MUNICÍPIO - IBGE':'city_ibge_code'})


# In[13]:


cidadesBrasil = cidadesBrasil.rename(columns={'MUNICÍPIO - TOM':'municipio'})


# In[14]:


dadosCovid = pd.merge(dadosCovid, cidadesBrasil, on="city_ibge_code")


# In[15]:


dadosCovid.info()


# In[16]:


municipios= pd.DataFrame(dadosCovid['city_ibge_code'].unique(),columns=['city_ibge_code'])


# In[17]:


municipios


# In[18]:


query = "Select distinct city_ibge_code, city from DADOS_COVID where place_type = 'city';"
municipios_covid = pd.read_sql(query,mydb)


# In[19]:


municipios_covid


# In[20]:


dimensao_municipios_covid = pd.merge(municipios_covid,cidadesBrasil, on='city_ibge_code')


# In[21]:


dimensao_municipios_covid = dimensao_municipios_covid.drop(columns='city')


dimensao_municipios_covid


# ### Criação dimensão UF

# In[22]:


query = "Select distinct city_ibge_code, state from DADOS_COVID where place_type = 'state';"
uf_covid = pd.read_sql(query,mydb)
mydb.close() #close the connection
uf_covid


# In[23]:


dimensao_uf_covid = uf_covid.rename(columns={'city_ibge_code' : 'state_ibge_code','state': 'estado'})

dimensao_uf_covid.to_csv('dim_uf_covid.csv', index=False)


# ### Criação da fato casos

# In[24]:


dadosCovid


# In[25]:


dadosCovid['date'] = pd.to_datetime(dadosCovid['date'])

dadosCovid


# In[26]:


dadosCovid['state_ibge_code'] =  dadosCovid['city_ibge_code']/100000

dadosCovid['state_ibge_code'] = dadosCovid['state_ibge_code'].astype(int)

dadosCovid


# In[27]:


fato_dados_covid = dadosCovid.drop(columns=['city','municipio','state','place_type','epidemiological_week',
                                                      'estimated_population','is_last','is_repeated',
                                                      'last_available_confirmed','last_available_confirmed_per_100k_inhabitants',
                                                      'last_available_date','last_available_death_rate','order_for_place',
                                                      'state'])


# In[28]:


fato_dados_covid.info()


# In[29]:


fato_dados_covid = fato_dados_covid.reset_index()

fato_dados_covid


# In[30]:


fato_dados_covid['index'] = fato_dados_covid['index']+1


# In[31]:


fato_dados_covid = fato_dados_covid.rename(columns={'index' : 'id'})

fato_dados_covid


# In[32]:


fato_dados_covid.to_csv('fato_dados_covid.csv', index=False)


# In[ ]:




