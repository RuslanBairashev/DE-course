INSERT INTO ITDE1.RUBA_REP_FRAUD( EVENT_DT, PASSPORT, FIO, PHONE, EVENT_TYPE, REPORT_DT )
SELECT 
	EVENT_DT, 
	passport_num, 
    FIO,
    PHONE,
	'1', 
	SYSDATE
FROM (
SELECT min(trans_date) AS EVENT_DT, passport_num, FIO, phone
FROM ITDE1.RUBA_DWH_FACT_TRANSACTIONS tr
INNER JOIN (
    SELECT card.card_num, ac.account_num, ac.client_id, ac.passport_num, ac.phone, ac.FIO
    FROM ITDE1.RUBA_DWH_DIM_CARDS_HIST card
    INNER JOIN (
        SELECT DISTINCT  cli.passport_num, cli.client_id, cli.phone, acc.account_num
        , TRIM(LAST_NAME) || ' ' || TRIM(FIRST_NAME) || ' ' || TRIM(PATRONYMIC) AS FIO
        FROM ITDE1.RUBA_DWH_DIM_ACCOUNTS_HIST acc
        INNER JOIN ITDE1.RUBA_DWH_DIM_CLIENTS_HIST cli
        ON acc.client = cli.client_id
        WHERE passport_valid_to < SYSDATE
    ) ac
    ON card.account_num = ac.account_num
) fr
ON tr.card_num = fr.card_num
GROUP BY passport_num, FIO, phone
)
UNION
SELECT 
	EVENT_DT, 
	frod.passport_num, 
    FIO,
    PHONE,
	'1', 
	SYSDATE
FROM ITDE1.ruba_dwh_fact_blacklist bl
INNER JOIN (
    SELECT min(trans_date) AS EVENT_DT, passport_num, FIO, phone --2
    FROM ITDE1.RUBA_DWH_FACT_TRANSACTIONS tr
    INNER JOIN (
        SELECT card.card_num, ac.account_num, ac.client_id, ac.passport_num, ac.phone, ac.FIO --1
        FROM ITDE1.RUBA_DWH_DIM_CARDS_HIST card
        INNER JOIN (
            SELECT DISTINCT  cli.passport_num, cli.client_id, cli.phone, acc.account_num
            , TRIM(LAST_NAME) || ' ' || TRIM(FIRST_NAME) || ' ' || TRIM(PATRONYMIC) AS FIO
            FROM ITDE1.RUBA_DWH_DIM_ACCOUNTS_HIST acc
            INNER JOIN ITDE1.RUBA_DWH_DIM_CLIENTS_HIST cli
            ON acc.client = cli.client_id
        ) ac
        ON card.account_num = ac.account_num --1
    ) fr 
    ON tr.card_num = fr.card_num
    GROUP BY passport_num, FIO, phone --2
) frod
ON bl.passport_num = frod.passport_num