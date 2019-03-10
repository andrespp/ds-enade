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

def extract(data_src, gzip=False):
    """Extract data from source

    Parameters
    ----------
        data_src : string
            Path to CSV file

        gzip : boolean, default False
            Whether CSV file is compressed or not.

    Returns
    -------
        data : Pandas DataFrame
            Extracted Data
    """

    if gzip:
        compress = 'gzip'
    compress = 'infer'

    return pd.read_csv(data_src, compression=compress, sep=';')

def transform(data, dim_groups):
    """Transform data

    Parameters
    ----------
        data | Pandas DataFrame
            Data to be transformed

        dim_groups | Pandas DataFrame
           dim_groups DataFrame

    Returns
    -------
        tdata : Pandas DataFrame
            Tranformed data
    """
    #################
    ## Data Selection

    # UFPA / UFOPA / UNIFESSPA / IFPA / UFRA / UNIFAP / UFT only
    # 569  / 15059 / 18440     / 1813 / 590  / 830    / 3849
    data = data[data['CO_IES'].isin([569, 15059, 18440, 1813, 590, 830, 3849])]

    # Absence evaluation
    data = data[data['TP_PRES'].isin([555])] # presente com resultado válido

    ##############
    ## New columns

    data['NM_IES'] = data['CO_IES'].apply(get_nm_ies)
    data['NM_GRUPO'] = data['CO_GRUPO'].apply(
        lambda x: dim_groups[dim_groups['CO_GRUPO'] == x].iloc[0, 1])
    data['NM_MODALIDADE'] = data['CO_MODALIDADE'].apply(get_nm_modalidade)
    data['NM_UF_CURSO'] = data['CO_UF_CURSO'].apply(get_nm_uf_curso)
    data['NM_SEXO'] = data['TP_SEXO'].apply(get_nm_sexo)
    data['NM_INSCRICAO'] = data['TP_INSCRICAO'].apply(get_nm_inscricao)
    data['NM_PRES'] = data['TP_PRES'].apply(get_nm_pres)

    #############################
    ## Select and reorder columns
    data = data[['NU_ANO',        # Ano da avaliação
                 'CO_IES',        # Código da IES (e-Mec)
                 'NM_IES',        # Nome das IES filtradas
                 'CO_GRUPO',      # Código da área de enquadramento do curso
                 'NM_GRUPO',      # Nome da área de enquadramento do curso
                 'CO_CURSO',      # Código do curso no Enade
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
    data.reset_index(inplace=True)

    return data

def load(data, fname):
    """Load data into the Parquet File

    Parameters
    ----------
        data : Pandas DataFrame
            Data to be written

        fname : string
            File name for output file
    """
    return data.to_parquet(fname, compression='gzip')

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

def get_nm_inscricao(tp_inscricao):
    """Returns "Tipo de inscrição do participante" description
    """
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
