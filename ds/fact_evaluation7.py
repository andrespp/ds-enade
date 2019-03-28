"""fact_evaluation7.py

    Data of ENADE's evalutions for the following IES:
        UFPA
        UFOPA
        UNIFESSPA
        IFPA
        UFRA
        UNIFAP
        UFT
"""
import pandas as pd
import numpy as np
from ds.dtypes import ENADE_DTYPE

def extract(data_src, gzip=False, decimal='.'):
    """Extract data from source

    Parameters
    ----------
        data_src : string
            Path to CSV file

        gzip : boolean, default False
            Whether CSV file is compressed or not.

        decimal : str, default '.'
            Character to recognize as decimal point.

    Returns
    -------
        data : Pandas DataFrame
            Extracted Data
    """

    if gzip:
        compress = 'gzip'
    compress = 'infer'
    
    cols = ['NU_ANO', 'CO_IES', 'CO_GRUPO', 'CO_CURSO', 'CO_MODALIDADE', \
            'CO_MUNIC_CURSO', 'CO_UF_CURSO', 'NU_IDADE', 'TP_SEXO', \
            'TP_INSCRICAO', 'TP_PRES', 'NT_GER', 'NT_FG', 'NT_CE']
    
    if (data_src == './datasrc/ENADE_2004.csv.gz') or (data_src == './datasrc/ENADE_2005.csv.gz') or \
       (data_src == './datasrc/ENADE_2006.csv.gz') or (data_src == './datasrc/ENADE_2007.csv.gz') or \
       (data_src == './datasrc/ENADE_2008.csv.gz') or (data_src == './datasrc/ENADE_2009.csv.gz'):
        cols = ['nu_ano', 'co_ies', 'co_grupo', 'co_curso',
                'co_uf_habil', 'co_munic_habil', 'nu_idade', 'tp_sexo',
                'in_grad', 'tp_pres', 'nt_ger', 'nt_fg', 'nt_ce']

    df = pd.read_csv(data_src, compression=compress, dtype=ENADE_DTYPE,
                     sep=';', decimal=decimal, usecols=cols)
    
    # Convert name of columns to uppercase			
    df.columns = map(str.upper, df.columns)
    
    # Standardized all columns in every years 
    if df['NU_ANO'][0] == 2004 or 2005 or 2006 or 2007 or 2008 or 2009:
        # Rename columns
        df.rename(columns = {'IN_GRAD':'TP_INSCRICAO',
                             'CO_UF_HABIL':'CO_UF_CURSO', 
                             'CO_MUNIC_HABIL':'CO_MUNIC_CURSO'}, inplace = True)

        # Between 2004 and 2009 only "modalidade presencial"
        df['CO_MODALIDADE'] = np.ones(len(df))
	
    if df['NU_ANO'][0] == 2005:
        # Fix year 2005 with values "-2005"
        df['NU_ANO'].replace([-2005], 2005, inplace=True) 
        
    if df['NU_ANO'][0] == 2016:
        # Corrige problema de 2016 com células contendo apenas espaços
        df['NT_GER'] = df['NT_GER'].replace(r'\s+', 0, regex=True)
        df['NT_FG'] = df['NT_FG'].replace(r'\s+', 0, regex=True)
        df['NT_CE'] = df['NT_CE'].replace(r'\s+', 0, regex=True)

        # Substitui "," por "."
        df['NT_GER'] = df['NT_GER'].str.replace(',', '.')
        df['NT_FG'] = df['NT_FG'].str.replace(',', '.')
        df['NT_CE'] = df['NT_CE'].str.replace(',', '.')

    df['CO_IES'] = df['CO_IES'].apply(aux_convert_dj1)
    df['CO_CURSO'] = df['CO_CURSO'].apply(aux_convert_dj1)
    df['CO_MUNIC_CURSO'] = df['CO_MUNIC_CURSO'].apply(aux_convert_dj1)

    df['NT_GER'] = df['NT_GER'].apply(pd.to_numeric, errors='coerce')
    df['NT_FG'] = df['NT_FG'].apply(pd.to_numeric, errors='coerce')
    df['NT_CE'] = df['NT_CE'].apply(pd.to_numeric, errors='coerce')
    return df

def transform(data, dim_groups, dim_areas):
    """Transform data

    Parameters
    ----------
        data | Pandas DataFrame
            Data to be transformed

        dim_groups | Pandas DataFrame
           dim_groups DataFrame

        dim_area | Pandas DataFrame
            dim_area DataFrame

    Returns
    -------
        tdata : Pandas DataFrame
            Tranformed data
    """
    #################
    ## Data Selection

    ## Federal University in Brazil
    ies = pd.read_csv('./datasrc/cod_ies.csv')
    ies = ies['COD'].tolist()
    
    data = data[data['CO_IES'].isin(ies)]
    
    ## Absence evaluation
    data = data[data['TP_PRES'].isin([555])] ## presente com resultado válido

    ##############
    ## New columns

    data['NM_IES'] = data['CO_IES'].apply(get_nm_ies)
    data['NM_GRUPO'] = data['CO_GRUPO'].apply(
        lambda x: dim_groups[dim_groups['CO_GRUPO'] == x].iloc[0, 1])
    data['NM_MODALIDADE'] = data['CO_MODALIDADE'].apply(get_nm_modalidade)
    data['NM_UF_CURSO'] = data['CO_UF_CURSO'].apply(get_nm_uf_curso)
    data['NM_SEXO'] = data['TP_SEXO'].apply(get_nm_sexo)
    data['NM_INSCRICAO'] = data.apply(lambda x:
                                      get_nm_inscricao(x['TP_INSCRICAO'],
                                                       x['NU_ANO']), axis=1)
    data['NM_PRES'] = data['TP_PRES'].apply(get_nm_pres)
    ## data['NT_GER'] = data['NT_GERAL'].astype('float64')
    ## data['NT_FG'] = data['NT_FG'].astype('float64')
    ## data['NT_CE'] = data['NT_CE'].astype('float64')

    ## Join with "co_area.csv to know which area the course belongs to
    data['CO_IES'] = data['CO_IES'].astype('int64')
    data['CO_MUNIC_CURSO'] = data['CO_MUNIC_CURSO'].astype('int64')
    data['CO_CURSO'] = data['CO_CURSO'].astype('int64')
    data = data.join(dim_areas.set_index('CO_CURSO'), on='CO_CURSO')

    #############################
    ## Select and reorder columns
    data = data[['NU_ANO',        # Ano da avaliação
                 'CO_IES',        # Código da IES (e-Mec)
                 'NM_IES',        # Nome das IES filtradas
                 'CO_GRUPO',      # Código do grupo de enquadramento do curso
                 'NM_GRUPO',      # Nome da área de enquadramento do curso
                 'CO_CURSO',      # Código do curso no Enade
                 'CO_AREA',       # Código da área a qual pertence o curso
                 'NM_AREA',       # Nome das áreas
                 'CO_MODALIDADE', # Código da modalidade de Ensino
                 'NM_MODALIDADE', # Nomes das modalidades
                 'CO_MUNIC_CURSO',# Cód do município de funcionamento do curso
                 'CO_UF_CURSO',   # Cód do município de funcionamento do curso
                 'NM_UF_CURSO',   # Nome da Unidade Federativa filtrada
                 'NU_IDADE',      # Idade do inscrito em 21/11/2010 (min 15, max 99)
                 'TP_SEXO',       # Sexo (M/F/N)
                 'NM_SEXO',       # Nomes dos sexos
                 'TP_INSCRICAO',  # Tipo de inscrição (0=Concluinte, 1=Ingressante)
                 'NM_INSCRICAO',  # Nome dos tipos de inscrições
                 'TP_PRES',       # Tipo de presença ENADE
                 'NM_PRES',       # Nome do tipo de presença no ENADE
                 'NT_GER',        # Nota bruta da prova
                 'NT_FG',         # Nota bruta da formação geral
                 'NT_CE',         # Nota bruta do conhecimento específico
                ]]
    data.reset_index(drop=True, inplace=True)

    return data

def load(data, fname, ftype='csv'):
    """Load data into the Parquet File

    Parameters
    ----------
        data : Pandas DataFrame
            Data to be written

        fname : string
            File name for output file

        ftype : string ,default 'csv'
            Type of the output file
    """
    print(data.dtypes)
    if ftype == 'csv':
        return data.to_csv(fname, sep=';', decimal=',', index=False)
    elif ftype == 'parquet':
        return data.to_parquet(fname, compression='gzip')
    else:
        return 'Not Implemented'

def get_nm_ies(co_ies):
    """Returns "As IES de interesse" description
    """
    switcher = {
        569:   'UFPA',
        15059: 'UFOPA',
        18440: 'UNIFESSPA',
        1813:  'IFPA',
        590:   'UFRA',
        830:   'UNIFAP',#'Privada s/ fins lucrativos',
        3849:  'UFT',#'Privada s/ fins lucrativos',
    }
    return switcher.get(co_ies, 'Outra')

def get_nm_categad(co_categad):
    """Returns "Código da categoria administrativa" description
        Note: works for 2010
    """
    switcher = {
        10001: 'Federal',
        10002: 'Estadual',
        10003: 'Municipal',
        10005: 'Privada',
        10006: 'Privada',
        10007: 'Privada',#'Privada s/ fins lucrativos',
        10008: 'Privada',#'Privada s/ fins lucrativos',
        10009: 'Privada',#'Privada s/ fins lucrativos',
        10036: 'Não diponível',
    }
    return switcher.get(co_categad, 'err')

def get_nm_modalidade(co_modalidade):
    """Returns "Modalidade do ensino" description
    """
    switcher = {
        0: 'EaD',
        1: 'Presencial',
    }
    return switcher.get(co_modalidade, 'err')

def get_nm_uf_curso(co_uf_curso):
    """Returns "Código da UF" description
    """
    switcher = {
        11: 'PA',
    }
    return switcher.get(co_uf_curso, 'Outro')

def get_nm_sexo(tp_sexo):
    """Returns "Tipo de sexo do participante" description
    """
    switcher = {
        'M': 'Masculino',
        'F': 'Feminino',
        'N': 'Outro'
    }
    return switcher.get(tp_sexo, 'Outro')

def get_nm_inscricao(tp_inscricao, ds_year):
    """Returns "Tipo de inscrição do participante" description
    """
    if ds_year == 2017:
        switcher = {
            1: 'Concluinte',
            0: 'Ingressante',
        }
    else:
        switcher = {
            0: 'Concluinte',
            1: 'Ingressante',
        }
    return switcher.get(tp_inscricao, 'err')

def get_nm_pres(tp_pres):
    """Returns "Tipo de presença no ENADE" description
    """
    switcher = {
        222: 'Ausente',
        555: 'Presente',
        556: 'Presente-Invalido'

    }
    return switcher.get(tp_pres, 'err')

def aux_convert_dj1(cod):
    """Returns -1 if cod == 0

        DJ1 (Identificação da IES retirada decorrente de decisão judicial)
    """
    if cod == 'DJ1':
        return -1
    return cod
