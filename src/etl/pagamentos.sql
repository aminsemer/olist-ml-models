-- Databricks notebook source



-- COMMAND ----------

--- CRIANDO A FEATURE STORE

WITH tb_pedidos AS (  
  
  SELECT 
       DISTINCT
       t1.idPedido,
       t2.idVendedor

  FROM silver.olist.pedido AS t1

    LEFT JOIN silver.olist.item_pedido AS t2 
    ON t1.idPedido = t2.idPedido

  WHERE t1.dtPedido < '2018-01-01'
    AND t1.dtPedido >= add_months('2018-01-01', -6)
    AND t2.idVendedor IS NOT NULL
),

tb_join AS (
  
  SELECT  t1.idVendedor,
          t2.*


  FROM tb_pedidos AS t1
    LEFT JOIN silver.olist.pagamento_pedido AS t2 ON t1.idPedido = t2.idPedido

),

tb_group AS (       
  
  SELECT idVendedor,
         descTipoPagamento,
         COUNT(DISTINCT idPedido) AS qtdePedidoMeioPagamento,
         ROUND(SUM(vlPagamento),1) AS vlPedidoMeioPagamento
  FROM tb_join

  GROUP BY idVendedor, descTipoPagamento
  ORDER BY idVendedor, descTipoPagamento

),


tb_summary AS (
  SELECT DISTINCT idVendedor,

                ---qtd pedido
                SUM(CASE WHEN descTipoPagamento = 'boleto' THEN qtdePedidoMeioPagamento ELSE 0 END) AS qtde_boleto_pedido,
                SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN qtdePedidoMeioPagamento ELSE 0 END) AS qtd_credit_card_pedido,
                SUM(CASE WHEN descTipoPagamento = 'voucher' THEN qtdePedidoMeioPagamento ELSE 0 END) AS qtd_voucher_pedido,
                SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN qtdePedidoMeioPagamento ELSE 0 END) AS debit_card,

                SUM(CASE WHEN descTipoPagamento = 'boleto' THEN qtdePedidoMeioPagamento ELSE 0 END) / SUM(qtdePedidoMeioPagamento) AS pct_qtd_boleto_pedido,
                SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN qtdePedidoMeioPagamento ELSE 0 END) / SUM(qtdePedidoMeioPagamento) AS pct_qtd_credit_card_pedido,
                SUM(CASE WHEN descTipoPagamento = 'voucher' THEN qtdePedidoMeioPagamento ELSE 0 END) / SUM(qtdePedidoMeioPagamento) AS pct_qtd_voucher_pedido,
                SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN qtdePedidoMeioPagamento ELSE 0 END) / SUM(qtdePedidoMeioPagamento) AS pct_qtd_debit_card,
                
                ---valor pedido

                SUM(CASE WHEN descTipoPagamento = 'boleto' THEN vlPedidoMeioPagamento ELSE 0 END) AS valor_boleto_pedido,
                SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN vlPedidoMeioPagamento ELSE 0 END) AS valor_credit_card_pedido,
                SUM(CASE WHEN descTipoPagamento = 'voucher' THEN vlPedidoMeioPagamento ELSE 0 END) AS valor_voucher_pedido,
                SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN vlPedidoMeioPagamento ELSE 0 END) AS valor_debit_card,

                SUM(CASE WHEN descTipoPagamento = 'boleto' THEN vlPedidoMeioPagamento ELSE 0 END) / SUM(vlPedidoMeioPagamento) AS pct_valor_boleto_pedido,
                SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN vlPedidoMeioPagamento ELSE 0 END) / SUM(vlPedidoMeioPagamento) AS pct_valor_credit_card_pedido,
                SUM(CASE WHEN descTipoPagamento = 'voucher' THEN vlPedidoMeioPagamento ELSE 0 END) / SUM(vlPedidoMeioPagamento) AS pct_valor_voucher_pedido,
                SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN vlPedidoMeioPagamento ELSE 0 END) / SUM(vlPedidoMeioPagamento) AS pct_valor_debit_card
                

  FROM tb_group

  GROUP BY idVendedor

),

tb_cartao AS (

  SELECT idVendedor,
       avg(nrParcelas) AS avgQtdeParcelas,
       median(nrParcelas) AS medianQtdeParcelas,
       max(nrParcelas) AS maxQtdeParcelas,
       min(nrParcelas) AS minQtdeParcelas
  FROM tb_join
  WHERE descTipoPagamento = 'credit_card'
  GROUP BY idVendedor

)

SELECT '2018-01-01' AS dtreference --- data de referencia para entendermos a fotografia do período da base
       t1.*,
       t2.avgQtdeParcelas,
       t2.medianQtdeParcelas,
       t2.maxQtdeParcelas,
       t2.minQtdeParcelas
       

FROM tb_summary AS t1 
  LEFT JOIN tb_cartao AS t2 ON t1.idVendedor = t2.idVendedor

