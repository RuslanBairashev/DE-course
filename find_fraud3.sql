INSERT INTO ITDE1.RUBA_REP_FRAUD( EVENT_DT, PASSPORT, FIO, PHONE, EVENT_TYPE, REPORT_DT )
select 
    event_dt,
    info.passport_num,
    info.fio,
    info.phone,
    '3',
    SYSDATE
from (
SELECT
    min(trans_date) AS EVENT_DT,
    card_num
FROM (
    select
        tot.card_num,
        tot.terminal_city,
        tot.trans_date,
        LAG(tot.trans_date) over 
        (partition by tot.card_num order by tot.trans_date) prev_dt,
        LAG(tot.terminal_city) over 
        (partition by tot.card_num order by tot.trans_date) prev_city
    FROM (
        SELECT
            t.trans_date,
            t.card_num,
            ter.terminal_city
        FROM itde1.ruba_dwh_fact_transactions t
        inner join itde1.ruba_dwh_dim_terminals_hist ter
        on t.terminal = ter.terminal_id
        ) tot
    ) end
where (end.trans_date - end.prev_dt) <= 1/24
and end.terminal_city <> end.prev_city
group by card_num
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
WHERE EVENT_DT >= (
select last_update
    from itde1.ruba_meta_loading
    where tablename = 'RUBA_DWH_DIM_TERMINALS_HIST'
)
