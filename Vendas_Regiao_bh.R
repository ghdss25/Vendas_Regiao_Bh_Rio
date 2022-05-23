setwd("/home/gustavo/Projeto de Dados/Análise_R/Vendas_Regiao")
getwd()

library(dplyr)
library(tidyverse)
library(RSQLite)

vendas_belo_horizonte <- read.csv("belohorizonte.csv", sep = ";")

View(vendas_belo_horizonte)

## Divisão de Colunas da Região Belo Horizonte 

Dim_Vendedor_BH <- data.frame(
  
  ID_Vendedor = vendas_belo_horizonte$ID.Vendedor, 
  Vendedor = vendas_belo_horizonte$Vendedor
)

Dim_Loja_BH <- data.frame(
  
  Loja = vendas_belo_horizonte$Loja, 
  Cidade = vendas_belo_horizonte$Cidade,
  Estado = vendas_belo_horizonte$Estado
  
)

Dim_Produto_BH <- data.frame(
  
  ID_Produto = vendas_belo_horizonte$ID.Produto,
  Categoria = vendas_belo_horizonte$Categoria, 
  Fabricante = vendas_belo_horizonte$Fabricante, 
  Produto = vendas_belo_horizonte$Produto, 
  Segmento = vendas_belo_horizonte$Segmento
  
)

Dim_Tempo <- data.frame(
  
  Data_Venda = vendas_belo_horizonte$Data.Venda
)

TB_Fato <- data.frame(
  
  Loja = vendas_belo_horizonte$Loja, 
  ID_Vendedor = vendas_belo_horizonte$ID.Vendedor, 
  ID_Produto = vendas_belo_horizonte$ID.Produto,
  Data_Venda = vendas_belo_horizonte$Data.Venda,
  Valor_Venda = vendas_belo_horizonte$ValorVenda
)

## Remoção de Dados Duplicados 
Dim_Vendedor_BH <- Dim_Vendedor_BH %>% distinct(ID_Vendedor, .keep_all = TRUE)
Dim_Loja_BH <- Dim_Loja_BH %>% distinct(Loja, .keep_all = TRUE)

## Visualização de Tabelas
View(Dim_Vendedor_BH)
View(Dim_Loja_BH)
View(Dim_Produto_BH)
View(Dim_Tempo)
View(TB_Fato)

## Conexão com Banco de dados da Planilha de Vendas Região 
db_bh <- dbConnect(SQLite(), dbname="/home/gustavo/Projeto de Dados/Análise_R/Vendas_Regiao.sqlite3")

## Criação de tabela para o  Banco de Dados 
dbWriteTable(db_bh, "fatos_bh", TB_Fato, overwrite = TRUE)
dbWriteTable(db_bh, "lojas_bh", Dim_Loja_BH, overwrite = TRUE)
dbWriteTable(db_bh, "produtos_bh", Dim_Produto_BH, overwrite = TRUE)
dbWriteTable(db_bh, "tempo_bh", Dim_Tempo, overwrite = TRUE)
dbWriteTable(db_bh, "vendedor_bh", Dim_Vendedor_BH, overwrite = TRUE)

## Seleção das Colunas para o banco de dados 
fatos_db_bh <- dbGetQuery(db_bh, "SELECT * FROM fatos_bh")
lojas_db_bh <- dbGetQuery(db_bh, "SELECT * FROM lojas_bh")
produtos_db_bh <- dbGetQuery(db_bh, "SELECT * FROM produtos_bh")
tempo_db_bh <- dbGetQuery(db_bh, "SELECT * FROM tempo_bh") 
vendedor_db_bh <- dbGetQuery(db_bh, "SELECT * FROM vendedor_bh")

## Visualização de Dados pelo Banco de Dados Sqlite
View(fatos_db)
View(lojas_db_bh)
View(produtos_db_bh)
View(tempo_db_bh)
View(vendedor_db_bh)

## Construção de Consulta do Banco de Dados com - Junção de Tabelas 

## Questões 

## 1 - Informe o total e média de Vendas por Categoria em BH
myquery <- "SELECT Categoria, SUM(Valor_Venda) AS Total, AVG(Valor_Venda) AS Média
           FROM fatos_bh f INNER JOIN produtos_bh p ON f.ID_Produto = p.ID_Produto 
           GROUP BY p.Categoria"

consulta_categoria_produto <- dbGetQuery(db_bh, myquery)

consulta_categoria_produto$Total <- paste("R$", format(consulta_categoria_produto$Total, decimal.mark = ",", big.mark = ".", nsmall = 3))
consulta_categoria_produto$Média <- paste("R$", format(consulta_categoria_produto$Média, decimal.mark = ",", big.mark = ".", nsmall = 3))

View(consulta_categoria_produto)

## 2 - Informe o total e média de Vendas por Segmento em BH 
myquery <- "SELECT Segmento, SUM(Valor_Venda) AS Total, AVG(Valor_Venda) AS Média 
            FROM fatos_bh f INNER JOIN produtos_bh p ON f.ID_Produto = p.ID_Produto 
            GROUP BY p.Segmento"

consulta_segmento_produto <- dbGetQuery(db_bh, myquery)

consulta_segmento_produto$Total <- paste("R$", format(consulta_segmento_produto$Total, decimal.mark = ",", big.mark = ".", nsmall = 3))
consulta_segmento_produto$Média <- paste("R$", format(consulta_segmento_produto$Média, decimal.mark = ",", big.mark = ".", nsmall = 3))

View(consulta_segmento_produto)

## 3 - Informe o total e média de Vendas por Vendedores em BH 
myquery <- "SELECT Vendedor, SUM(Valor_venda) AS Total, AVG(Valor_Venda) AS Média 
            FROM fatos_bh f INNER JOIN vendedor_bh v ON f.ID_Vendedor = v.ID_Vendedor
            GROUP BY v.Vendedor"

consulta_vendedor <- dbGetQuery(db_bh, myquery)

View(consulta_vendedor)

## 4 - Informe o total, média, desvio padrão de vendas por Cidade, Estado, Fabricante 
myquery <- "SELECT Estado, Cidade, Fabricante, ROUND(SUM(Valor_Venda), 2) AS Total, ROUND(AVG(Valor_Venda), 2) AS Média,
            ROUND(STDEV(Valor_Venda),2) AS Desvio_Padrão FROM fatos_bh f INNER JOIN lojas_bh l ON f.Loja = l.Loja
            INNER JOIN produtos_bh p ON f.ID_Produto = p.ID_Produto GROUP BY Fabricante"

consulta_cidade_estado_fabricante <- dbGetQuery(db_bh, myquery)

consulta_cidade_estado_fabricante$Total <- paste("R$", format(consulta_cidade_estado_fabricante$Total, decimal.mark = ",", big.mark = ".", nsmall = 3))
consulta_cidade_estado_fabricante$Média <- paste("R$", format(consulta_cidade_estado_fabricante$Média, decimal.mark = ",", big.mark = ".", nsmall = 3))
consulta_cidade_estado_fabricante$Desvio_Padrão <- paste("R$", format(consulta_cidade_estado_fabricante$Desvio_Padrão, decimal.mark = ",", big.mark = ".", nsmall = 3))

View(consulta_cidade_estado_fabricante)

## 5 - Informe o total, média de vendas por Cidade, Estado e Vendedor, Segmento e Categoria 
myquery <- "SELECT Estado, Cidade, Segmento, Categoria, Vendedor, ROUND(SUM(Valor_Venda), 2) AS Total, 
            ROUND(AVG(Valor_Venda), 2) AS Média, ROUND(STDEV(Valor_Venda),2) AS Desvio_Padrão FROM fatos_bh f 
            INNER JOIN lojas_bh l ON f.Loja = l.Loja 
            INNER JOIN produtos_bh p ON f.ID_Produto = p.ID_Produto 
            INNER JOIN vendedor_bh v ON f.ID_Vendedor = v.ID_Vendedor GROUP BY Vendedor"

consulta_cidade_estado_segmento_vendedor <- dbGetQuery(db_bh, myquery)

consulta_cidade_estado_segmento_vendedor$Total <- paste("R$", format(consulta_cidade_estado_segmento_vendedor$Total, decimal.mark = ",", big.mark = ".", nsmall = 3))
consulta_cidade_estado_segmento_vendedor$Média <- paste("R$", format(consulta_cidade_estado_segmento_vendedor$Média, decimal.mark = ",", big.mark = ".", nsmall = 3))
consulta_cidade_estado_segmento_vendedor$Desvio_Padrão <- paste("R$", format(consulta_cidade_estado_segmento_vendedor$Desvio_Padrão, decimal.mark = ",", big.mark = ".", nsmall = 3))

View(consulta_cidade_estado_segmento_vendedor)
