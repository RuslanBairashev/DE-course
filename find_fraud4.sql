INSERT INTO ITDE1.RUBA_REP_FRAUD( EVENT_DT, PASSPORT, FIO, PHONE, EVENT_TYPE, REPORT_DT )
select 
    events.trans_date,
    info.passport_num,
    info.fio,
    info.phone,
    '4',
    SYSDATE
from (
--
select *
from
(
select
    tot.card_num,
    tot.trans_date,
    tot.oper_result,
    LAG(tot.oper_result,1) over 
    (partition by tot.card_num order by tot.trans_date) prev_res,
    LAG(tot.oper_result,2) over 
    (partition by tot.card_num order by tot.trans_date) prev_res2,
    LAG(tot.oper_result,3) over 
    (partition by tot.card_num order by tot.trans_date) prev_res3,
    LAG(tot.trans_date,3) over 
    (partition by tot.card_num order by tot.trans_date) prev_dt
FROM (
    SELECT
        t.trans_date,
        t.card_num,
        t.oper_result
    FROM itde1.ruba_dwh_fact_transactions t
    ) tot
) end
where 1=1
AND end.oper_result = 'SUCCESS'
AND end.prev_res = 'REJECT'
AND end.prev_res2 = 'REJECT'
AND end.prev_res3 = 'REJECT'
AND (end.trans_date - end.prev_dt) < 1/72
    --
) events
inner join (
    SELECT card.card_num, ac.account_num, ac.client_id, ac.passport_num, ac.phone, ac.FIO
    FROM ITDE1.RUBA_DWH_DIM_CARDS_HIST card
    INNER JOIN (
        SELECT DISTINCT  cli.passport_num, cli.client_id, cli.phone, acc.account_num
        , TRIM(LAST_NAME) || ' ' || TRIM(FIRST_NAME) || ' ' || TRIM(PATRONYMIC) AS FIO
        FROM ITDE1.RUBA_DWH_DIM_ACCOUNTS_HIST acc
        INNER JOIN ITDE1.RUBA_DWH_DIM_CLIENTS_HIST cli
        ON acc.client = cli.client_id
    ) ac
    ON card.account_num = ac.account_num
    ) info
on events.card_num = info.card_num
WHERE events.trans_date >= (
select last_update
    from itde1.ruba_meta_loading
    where tablename = 'RUBA_DWH_DIM_TERMINALS_HIST'
)