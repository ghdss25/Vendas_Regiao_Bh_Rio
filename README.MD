## Sobre o projeto 

** Conjunto de dados distribuído pela Data Science Academy, onde mostra as vendas de produtos, segmento, fabricante e lojas pelos estados de Belo Horizonte e Rio de Janeiro.

## Tarefas para a planilha de Vendas na Região de Belo Horizonte


** O total e média de vendas por Categoria em BH ** 
   
** O total e média de Vendas por Segmento em BH  ** 

** O total e média de Vendas por Vendedores em BH  ** 

** o total, média e desvio padrão de vendas por Cidade, Estado, Fabricante ** 

** o total, média de vendas por Cidade, Estado e Vendedor, Segmento e Categoria **

## Tarefas para a planilha de Vendas na Região do Rio de Janeiro 


** O total e média de vendas por Categoria em RJ ** 
   
** O total e média de Vendas por Segmento em RJ  ** 

** O total e média de Vendas por Vendedores em RJ  ** 

** o total, média e desvio padrão de vendas por Cidade, Estado, Fabricante ** 

** o total, média de vendas por Cidade, Estado e Vendedor, Segmento e Categoria **


## Passos para a tarefa 

    1 - Importando o caminho do disco para a Linguagem 
    2 - Lendo o caminho do disco 
    3 - Lendo as bibliotecas para a importação, manipulação e agrupamento dos dados no banco de dados
    4 - Divisão de Colunas da Região de Belo Horizonte, na intenção de criar entidade e relacionamento de tabelas para um banco de dados
    5 - Remoção de Dados Duplicados 
    6 - Visualização de Tabelas
    7 - Conexão com Banco de dados da Planilha de Vendas Região de Belo Horizonte
    8 - Criação de tabela para o  Banco de Dados 
    9 - Seleção das Colunas para o banco de dados 
    10 - Visualização do banco de dados 
    11 - Construção de Consulta do Banco de Dados com a Junção de Tabelas 
    12 - Instalação do Banco de dados do Sqlite no Sistema Operacional Windows 
    13 - Instalação do ODBC no Sistema Operacional Windows - padrão para acesso a sistemas gerenciadores de bancos de dados. Define um conjunto de interfaces que permitem o uso na linguagem de programação. 
    14 - Adicionando os arquivo de database no sistema ODBC - Vendas_Regiao.sqlite3 
    15 - Conexão do Sistema gerenciador de banco de dados ODBC para o arquivo Vendas_Regiao.sqlite3 por um servidor local no Power BI 
    16 - Mudança de tipos de dados das tabelas fatos_bh: na coluna Valor_Venda, Dim_tempo_bh: Data_Venda, Dim_loja_bh: Loja. 
    17 - Mudança de tipos de dados das tabelas fatos_rio: na coluna Valor_Venda, Dim_tempo_rio: Data_Venda, Dim_loja_rio: Loja. 
    18 - Criação de Dashboards no Power BI, vindo da base de dados do sistema ODBC do sqlite 
    
## Tecnologias Utilizadas 

** O projeto foi desenvolvido com as seguintes tecnologia ** 

- [R](https://www.r-project.org/)

- [SQLite](https://www.sqlite.org/index.html)

- [SQLite ODBC Driver](http://www.ch-werner.de/sqliteodbc/) 

- [PowerBI](https://powerbi.microsoft.com/pt-br/)

## Pacotes Utilizados 

** Pacotes para a sessão de conversão de Strings

- [stringr](https://stringr.tidyverse.org/)

** Pacote para consultas de colunas 

- [tidyverse](https://www.tidyverse.org/)
- [dplyr](https://dplyr.tidyverse.org/) 

** Pacote para a criação de Banco de Dados da linguagem R para o Sqlite3

-[RSQLite](https://cran.r-project.org/web/packages/RSQLite/vignettes/RSQLite.html)

## Apresentação do Dashboards no Power BI - Vendas nas Regiões do Rio de Janeiro e Belo Horizonte 

https://www.youtube.com/watch?v=jGnyo7znnnE

Desenvolvido por Gustavo Henrique de Souza Silva
