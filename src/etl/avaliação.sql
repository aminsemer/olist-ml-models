-- Databricks notebook source
WITH tb_pedido AS (

  SELECT DISTINCT 
         t1.idPedido,
         t2.idVendedor
  FROM silver.olist.pedido AS t1

    LEFT JOIN silver.olist.item_pedido AS t2 
      ON t1.idPedido = t2.idPedido

    --LEFT JOIN silver.olist.cliente AS t3 
    --  ON t1.idCliente = t3.idCliente

  WHERE t1.dtPedido <= '2018-01-01'
    AND t1.dtPedido >= add_months('2018-01-01', -6)
    AND idVendedor IS NOT NULL
),

tb_join AS (
  SELECT t1.*,
         t2.vlNota 
  FROM tb_pedido AS t1
    LEFT JOIN silver.olist.avaliacao_pedido AS t2 
      ON t1.idPedido = t2.idPedido
),

  tb_summary AS (
  SELECT idVendedor,
         AVG(vlNota) AS avgNota,
         percentile(vlNota, 0.5) AS medianNota,
         min(vlNota) AS minNota,
         max(vlNota) AS maxNota,
         COUNT(vlNota) / COUNT(idPedido) AS pctAvaliacao ---%de pedidos que possuem avaliação

  FROM tb_join
  GROUP BY idVendedor
  )

  SELECT '2018-01-01' AS dtReference,
          *
  FROM tb_summary
0 comments on commit 03237b4
@aminsemer
Comment
 
Leave a comment
 
 You’re receiving notifications because you’re watching this repository.
Footer
© 2023 GitHub, Inc.
Footer navigation
Terms
Privacy
Security
Status
Docs
Contact GitHub
Pricing
API
Training
Blog
About
Avalicao tá on · aminsemer/olist-ml-models@03237b4
