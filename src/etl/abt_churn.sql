-- Databricks notebook source
---Como não tenho autorização, o teo crio as tabela abt_chunr joinando com a tabela de vendas (que também foi criada)


WITH tb_activate AS (
  SELECT DISTINCT idVendedor,
         min(date(dtPedido)) AS dtAtivacao
         
  
  FROM silver.olist.pedido AS t1
    LEFT JOIN silver.olist.item_pedido AS t2 
    ON t1.idPedido = t2.idPedido
  
  WHERE t1.dtPedido >= '2018-01-01'
    AND t1.dtPedido <= date_add('2018-01-01', 45) --- dias de venda dos vendedores, pra cada vendedor todos os dias que ele vendeu.
    AND t2.idVendedor IS NOT NULL
  GROUP BY 1
  )

SELECT t1.*,
       t2.*,
       t3.*,
       t4.*,
       t5.*,
       t6.*,
       t7.* 
       CASE WHEN t2.idVendedor IS NULL THEN 1 ELSE O END AS flChurn 

FROM silver.analytics.fs_vendedor_vendas
  LEFT JOIN tb_activate AS t2 ON t1.idVendedor = t2.idVendedor
                              AND date_diff(dtAtivacao, dtReference) + qtdRecencia <=45

  LEFT JOIN silver.analytics.fs_vendedor_avaliacao AS t3 ON t1.idVendedor = t3.idVendedor
                              AND t1.dtReference = t3.dtReference

  LEFT JOIN silver.analytics.fs_vendedor_cliente AS t4 ON t1.idVendedor = t4.idVendedor
                              AND t1.dtReference = t4.dtReference

  LEFT JOIN silver.analytics.fs_vendedor_entrega AS t5 ON t1.idVendedor = t5.idVendedor
                              AND t1.dtReference = t5.dtReference

  LEFT JOIN silver.analytics.fs_vendedor_pagamentos AS t6 ON t1.idVendedor = t6.idVendedor
                              AND t1.dtReference = t6.dtReference

  LEFT JOIN silver.analytics.fs_vendedor_produto AS t7 ON t1.idVendedor = t7.idVendedor
                              AND t1.dtReference = t7.dtReference 

WHERE qtdRecencia <= 45
