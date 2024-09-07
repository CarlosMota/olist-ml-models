with tb_join as (
    select t2.*,
        t3.*
    from tb_orders t1
        left join tb_order_payments t2 on t1.order_id = t2.order_id
        left join tb_order_items t3 on t1.order_id = t3.order_id
    WHERE date(order_purchase_timestamp) BETWEEN date('2018-01-01', '-6 months') and date('2017-12-31')
        and t3.seller_id is not null
    group BY order_purchase_timestamp
    order by order_purchase_timestamp
),
tb_group as (
    select seller_id as idVendedor,
        payment_type as descTipoPagamento,
        count(DISTINCT order_id) as qtdPedidoMeioPagamento,
        sum(payment_value) as vlPedidoMeioPagamento
    from tb_join
    GROUP BY seller_id,
        payment_type
    order by seller_id,
        payment_type
)
select idVendedor,
    sum(
        case
            when descTipoPagamento = 'credit_card' then qtdPedidoMeioPagamento
            else 0
        end
    ) as qtde_credit_card,
    sum(
        case
            when descTipoPagamento = 'boleto' then qtdPedidoMeioPagamento
            else 0
        end
    ) as qtde_boleto,
    sum(
        case
            when descTipoPagamento = 'debit_card' then qtdPedidoMeioPagamento
            else 0
        end
    ) as qtde_debit_card,
    sum(
        case
            when descTipoPagamento = 'voucher' then qtdPedidoMeioPagamento
            else 0
        end
    ) as qtde_voucher,
    sum(
        case
            when descTipoPagamento = 'credit_card' then vlPedidoMeioPagamento
            else 0
        end
    ) as valor_credit_card,
    sum(
        case
            when descTipoPagamento = 'boleto' then vlPedidoMeioPagamento
            else 0
        end
    ) as valor_boleto,
    sum(
        case
            when descTipoPagamento = 'debit_card' then vlPedidoMeioPagamento
            else 0
        end
    ) as valor_debit_card,
    sum(
        case
            when descTipoPagamento = 'voucher' then vlPedidoMeioPagamento
            else 0
        end
    ) as valor_voucher,
    sum(
        case
            when descTipoPagamento = 'credit_card' then qtdPedidoMeioPagamento
            else 0
        end
    ) / sum(qtdPedidoMeioPagamento) as pct_qtd_credit_card,
    sum(
        case
            when descTipoPagamento = 'boleto' then qtdPedidoMeioPagamento
            else 0
        end
    ) / sum(qtdPedidoMeioPagamento) as pct_qtd_boleto,
    sum(
        case
            when descTipoPagamento = 'debit_card' then qtdPedidoMeioPagamento
            else 0
        end
    ) / sum(qtdPedidoMeioPagamento) as pct_qtd_debit_card,
    sum(
        case
            when descTipoPagamento = 'voucher' then qtdPedidoMeioPagamento
            else 0
        end
    ) / sum(qtdPedidoMeioPagamento) as pct_qtd_voucher,
    sum(
        case
            when descTipoPagamento = 'boleto' then vlPedidoMeioPagamento
            else 0
        end
    ) / sum(vlPedidoMeioPagamento) as pct_valor_boleto,
    sum(
        case
            when descTipoPagamento = 'debit_card' then vlPedidoMeioPagamento
            else 0
        end
    ) / sum(vlPedidoMeioPagamento) as pct_valor_debit_card,
    sum(
        case
            when descTipoPagamento = 'voucher' then vlPedidoMeioPagamento
            else 0
        end
    ) / sum(vlPedidoMeioPagamento) as pct_valor_voucher,
    sum(
        case
            when descTipoPagamento = 'credit_card' then vlPedidoMeioPagamento
            else 0
        end
    ) / sum(vlPedidoMeioPagamento) as pct_valor_credit_card
from tb_group
group BY idvendedor