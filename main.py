#!/user/bin/python

import jaydebeapi
import pandas as pd
import os
import glob

# Установка соединения с БД
conn = jaydebeapi.connect( 
'oracle.jdbc.driver.OracleDriver',
'',
['', ''],
'ojdbc8.jar'
)
curs = conn.cursor()

# Очистка STG таблиц и DWH FACT
curs.execute( 'TRUNCATE TABLE ITDE1.RUBA_STG_BLACKLIST' )
curs.execute( 'TRUNCATE TABLE ITDE1.RUBA_STG_TERMINALS' )
curs.execute( 'TRUNCATE TABLE ITDE1.RUBA_STG_TRANSACTIONS' )
curs.execute( 'TRUNCATE TABLE ITDE1.RUBA_STG_ACCOUNTS' )
curs.execute( 'TRUNCATE TABLE ITDE1.RUBA_STG_CARDS' )
curs.execute( 'TRUNCATE TABLE ITDE1.RUBA_STG_CLIENTS' )
curs.execute( 'TRUNCATE TABLE ITDE1.RUBA_DWH_FACT_BLACKLIST' )
curs.execute( 'TRUNCATE TABLE ITDE1.RUBA_STG_TERMINALS_RAW' )

# Загрузка из сырых файлов
file1=str(glob.glob("passport_blacklist*.xlsx")).strip('[').strip(']').strip('\'')
file2=str(glob.glob("terminals*.xlsx")).strip('[').strip(']').strip('\'')
file3=str(glob.glob("transactions*.txt")).strip('[').strip(']').strip('\'')
date_term = file2.replace('.xlsx','').replace('terminals_','')

df=pd.read_excel( file1, dtype={'date':str, 'passport' : str} )
curs.executemany( "INSERT INTO ITDE1.RUBA_STG_BLACKLIST values (?,?)", df.values.tolist() )

df=pd.read_excel( file2 )
curs.executemany( "INSERT INTO ITDE1.RUBA_STG_TERMINALS values (?,?,?,?)", df.values.tolist() )

df=pd.read_csv( file3, delimiter=';' )
curs.executemany( "INSERT INTO ITDE1.RUBA_STG_TRANSACTIONS values (?,?,?,?,?,?,?)", df.values.tolist() )

curs.execute(" UPDATE ITDE1.RUBA_META_LOADING SET last_update = to_date( '{0}', 'DDMMYYYY' ) WHERE dbname = 'ITDE1' AND tablename = 'RUBA_DWH_DIM_TERMINALS_HIST' ".format(date_term))

# Переименование и перемещение сырцов
os.rename(file1, './archive/' + file1 + '.backup')
os.rename(file2, './archive/' + file2 + '.backup')
os.rename(file3, './archive/' + file3 + '.backup')

# 2. Загрузка данных из BANK в STG

curs.execute("""
INSERT INTO ITDE1.RUBA_STG_TERMINALS_RAW( TERMINAL_ID, TERMINAL_TYPE, TERMINAL_CITY, TERMINAL_ADDRESS )
SELECT 
	TERMINAL_ID, 
	TERMINAL_TYPE, 
	TERMINAL_CITY, 
	TERMINAL_ADDRESS
FROM ITDE1.RUBA_STG_TERMINALS
""")

# Удаление дубликатов из STG терманалов
curs.execute("""
DELETE FROM ITDE1.RUBA_STG_TERMINALS
WHERE EXISTS
    ( 
    SELECT *
    FROM ITDE1.RUBA_DWH_DIM_TERMINALS_HIST
    WHERE ITDE1.RUBA_DWH_DIM_TERMINALS_HIST.TERMINAL_ID = ITDE1.RUBA_STG_TERMINALS.TERMINAL_ID
    AND ITDE1.RUBA_DWH_DIM_TERMINALS_HIST.TERMINAL_TYPE = ITDE1.RUBA_STG_TERMINALS.TERMINAL_TYPE
    AND ITDE1.RUBA_DWH_DIM_TERMINALS_HIST.TERMINAL_CITY = ITDE1.RUBA_STG_TERMINALS.TERMINAL_CITY
    AND ITDE1.RUBA_DWH_DIM_TERMINALS_HIST.TERMINAL_ADDRESS = ITDE1.RUBA_STG_TERMINALS.TERMINAL_ADDRESS
    )
""")

curs.execute("""
INSERT INTO ITDE1.RUBA_STG_ACCOUNTS( ACCOUNT_NUM, VALID_TO, CLIENT, CREATE_DT, UPDATE_DT )
SELECT 
	ACCOUNT, 
	VALID_TO, 
	CLIENT, 
	CREATE_DT, 
	UPDATE_DT
FROM BANK.ACCOUNTS
WHERE COALESCE( UPDATE_DT, CREATE_DT ) > (
	SELECT LAST_UPDATE FROM ITDE1.RUBA_META_LOADING WHERE DBNAME = 'ITDE1' AND TABLENAME = 'RUBA_DWH_DIM_ACCOUNTS_HIST'
)
""")

curs.execute("""
INSERT INTO ITDE1.RUBA_STG_CARDS( CARD_NUM, ACCOUNT, CREATE_DT, UPDATE_DT )
SELECT 
	REPLACE(CARD_NUM, ' '), 
	ACCOUNT, 
	CREATE_DT, 
	UPDATE_DT
FROM BANK.CARDS
WHERE COALESCE( UPDATE_DT, CREATE_DT ) > (
	SELECT LAST_UPDATE FROM ITDE1.RUBA_META_LOADING WHERE DBNAME = 'ITDE1' AND TABLENAME = 'RUBA_DWH_DIM_CARDS_HIST'
)
""")

curs.execute("""
INSERT INTO ITDE1.RUBA_STG_CLIENTS( CLIENT_ID, LAST_NAME, FIRST_NAME, PATRONYMIC, DATE_OF_BIRTH,
 PASSPORT_NUM, PASSPORT_VALID_TO, PHONE, CREATE_DT, UPDATE_DT )
SELECT 
	CLIENT_ID, 
	LAST_NAME, 
	FIRST_NAME, 
	PATRONYMIC, 
	DATE_OF_BIRTH, 
	PASSPORT_NUM, 
	PASSPORT_VALID_TO, 
	PHONE, 
	CREATE_DT, 
	UPDATE_DT
FROM BANK.CLIENTS
WHERE COALESCE( UPDATE_DT, CREATE_DT ) > (
	SELECT LAST_UPDATE FROM ITDE1.RUBA_META_LOADING WHERE DBNAME = 'ITDE1' AND TABLENAME = 'RUBA_DWH_DIM_CLIENTS_HIST'
)
""")

# 3. Обновляем обновленные строки в хранилище

# Вставка фактов

curs.execute("""
INSERT INTO ITDE1.RUBA_DWH_FACT_TRANSACTIONS( TRANS_ID, TRANS_DATE, CARD_NUM, OPER_TYPE, AMT, OPER_RESULT, TERMINAL )
SELECT
	TRANS_ID,
	TO_DATE ( TRANS_DATE,'YYYY-MM-DD HH24:MI:SS' ),
	REPLACE(CARD_NUM, ' '),
	OPER_TYPE,
	TO_NUMBER (TRIM(REPLACE(AMT, ',', '.'))),
	OPER_RESULT, 
	TERMINAL
FROM ITDE1.RUBA_STG_TRANSACTIONS
""")

curs.execute("""
INSERT INTO ITDE1.RUBA_DWH_FACT_BLACKLIST( PASSPORT_NUM, ENTRY_DT )
SELECT
	PASSPORT_NUM,
	TO_DATE ( REPORT_DT,'YYYY-MM-DD HH24:MI:SS' )
FROM ITDE1.RUBA_STG_blacklist
""")

#-- Загрузка измерений
#--------------------------------------------------------
#--  INCREMENT for Table TERMINALS
#--------------------------------------------------------
curs.execute("""
INSERT INTO ITDE1.RUBA_DWH_DIM_TERMINALS_HIST ( TERMINAL_ID, TERMINAL_TYPE, TERMINAL_CITY, TERMINAL_ADDRESS, EFFECTIVE_FROM, EFFECTIVE_TO, DELETED_FLG )
SELECT
	TERMINAL_ID, 
	TERMINAL_TYPE, 
	TERMINAL_CITY, 
	TERMINAL_ADDRESS,
	(
		SELECT last_update 
    	FROM ITDE1.RUBA_META_LOADING
		WHERE dbname = 'ITDE1' 
		AND tablename = 'RUBA_DWH_DIM_TERMINALS_HIST' 
	),
	TO_DATE('2999.12.31', 'YYYY.MM.DD'),
	'N'
FROM ITDE1.RUBA_STG_TERMINALS
""")

curs.execute("""
MERGE INTO ITDE1.RUBA_DWH_DIM_TERMINALS_HIST tgt
USING ITDE1.RUBA_STG_TERMINALS stg
ON ( tgt.TERMINAL_ID = stg.TERMINAL_ID AND tgt.EFFECTIVE_FROM < (
		SELECT last_update 
    	FROM ITDE1.RUBA_META_LOADING
		WHERE dbname = 'ITDE1' 
		AND tablename = 'RUBA_DWH_DIM_TERMINALS_HIST' 
	))
WHEN MATCHED THEN UPDATE SET
	tgt.EFFECTIVE_TO = (
		SELECT last_update 
    	FROM ITDE1.RUBA_META_LOADING
		WHERE dbname = 'ITDE1' 
		AND tablename = 'RUBA_DWH_DIM_TERMINALS_HIST' 
	) - INTERVAL '1' second
	WHERE tgt.EFFECTIVE_TO = TO_DATE('2999.12.31', 'YYYY.MM.DD')
""")

#--------------------------------------------------------
#--  INCREMENT for Table ACCOUNTS
#--------------------------------------------------------
curs.execute("""
INSERT INTO ITDE1.RUBA_DWH_DIM_ACCOUNTS_HIST( ACCOUNT_NUM, VALID_TO, CLIENT, EFFECTIVE_FROM, EFFECTIVE_TO, DELETED_FLG )
SELECT 
	ACCOUNT_NUM, 
	VALID_TO, 
	CLIENT, 
	COALESCE( UPDATE_DT, CREATE_DT ), --поменять на UPDATE_DT
	TO_DATE('2999.12.31', 'YYYY.MM.DD'),
	'N'
FROM ITDE1.RUBA_STG_ACCOUNTS
""")

curs.execute("""
MERGE INTO ITDE1.RUBA_DWH_DIM_ACCOUNTS_HIST tgt
USING ITDE1.RUBA_STG_ACCOUNTS stg
ON ( tgt.ACCOUNT_NUM = stg.ACCOUNT_NUM AND tgt.EFFECTIVE_FROM < COALESCE( UPDATE_DT, CREATE_DT ))
WHEN MATCHED THEN UPDATE SET
	tgt.EFFECTIVE_TO = COALESCE( stg.UPDATE_DT, stg.CREATE_DT ) - INTERVAL '1' second
	WHERE tgt.EFFECTIVE_TO = TO_DATE('2999.12.31', 'YYYY.MM.DD')
""")

#--------------------------------------------------------
#--  INCREMENT for Table PAYMENT_CARDS
#--------------------------------------------------------
curs.execute("""
INSERT INTO ITDE1.RUBA_DWH_DIM_CARDS_HIST ( CARD_NUM, ACCOUNT_NUM, EFFECTIVE_FROM, EFFECTIVE_TO, DELETED_FLG )
SELECT 
	CARD_NUM, 
	ACCOUNT, 
	COALESCE( UPDATE_DT, CREATE_DT ), --поменять на UPDATE_DT
	TO_DATE('2999.12.31', 'YYYY.MM.DD'),
	'N'
FROM ITDE1.RUBA_STG_CARDS
""")

curs.execute("""
MERGE INTO ITDE1.RUBA_DWH_DIM_CARDS_HIST tgt
USING ITDE1.RUBA_STG_CARDS stg
ON ( tgt.CARD_NUM = stg.CARD_NUM AND tgt.EFFECTIVE_FROM < COALESCE( UPDATE_DT, CREATE_DT ))
WHEN MATCHED THEN UPDATE SET
	tgt.EFFECTIVE_TO = COALESCE( stg.UPDATE_DT, stg.CREATE_DT ) - INTERVAL '1' second
	WHERE tgt.EFFECTIVE_TO = TO_DATE('2999.12.31', 'YYYY.MM.DD')
""")

#--------------------------------------------------------
#--  INCREMENT for Table CLIENTS
#--------------------------------------------------------
curs.execute("""
INSERT INTO ITDE1.RUBA_DWH_DIM_CLIENTS_HIST ( CLIENT_ID, LAST_NAME, FIRST_NAME, PATRONYMIC, DATE_OF_BIRTH,
 PASSPORT_NUM, PASSPORT_VALID_TO, PHONE, EFFECTIVE_FROM, EFFECTIVE_TO, DELETED_FLG )
SELECT
	CLIENT_ID, 
	LAST_NAME, 
	FIRST_NAME, 
	PATRONYMIC, 
	DATE_OF_BIRTH, 
	PASSPORT_NUM, 
	PASSPORT_VALID_TO, 
	PHONE, 
	COALESCE( UPDATE_DT, CREATE_DT ), --поменять на UPDATE_DT
	TO_DATE('2999.12.31', 'YYYY.MM.DD'),
	'N'
FROM ITDE1.RUBA_STG_CLIENTS
""")

curs.execute("""
MERGE INTO ITDE1.RUBA_DWH_DIM_CLIENTS_HIST tgt
USING ITDE1.RUBA_STG_CLIENTS stg
ON ( tgt.CLIENT_ID = stg.CLIENT_ID AND tgt.EFFECTIVE_FROM < COALESCE( UPDATE_DT, CREATE_DT ))
WHEN MATCHED THEN UPDATE SET
	tgt.EFFECTIVE_TO = COALESCE( stg.UPDATE_DT, stg.CREATE_DT ) - INTERVAL '1' second
	WHERE tgt.EFFECTIVE_TO = TO_DATE('2999.12.31', 'YYYY.MM.DD')
""")

#-- 4 и 5. Захватываем ключи для проверки удалений и Удаляем удаленные записи в целевой таблице

#--------------------------------------------------------
#--   TERMINALS
#--------------------------------------------------------
curs.execute( 'TRUNCATE TABLE ITDE1.RUBA_STG_DEL' )
curs.execute("""
INSERT INTO ITDE1.RUBA_STG_DEL ( ID )
SELECT TERMINAL_ID FROM ITDE1.RUBA_STG_TERMINALS_RAW
""")
curs.execute("""
INSERT INTO ITDE1.RUBA_DWH_DIM_TERMINALS_HIST ( TERMINAL_ID, TERMINAL_TYPE, TERMINAL_CITY, TERMINAL_ADDRESS, EFFECTIVE_FROM, EFFECTIVE_TO, DELETED_FLG )
SELECT
	t.TERMINAL_ID, 
	t.TERMINAL_TYPE, 
	t.TERMINAL_CITY, 
	t.TERMINAL_ADDRESS,
	(
		SELECT last_update 
    	FROM ITDE1.RUBA_META_LOADING
		WHERE dbname = 'ITDE1' 
		AND tablename = 'RUBA_DWH_DIM_TERMINALS_HIST' 
	),
	TO_DATE('2999.12.31', 'YYYY.MM.DD'),
	'Y'
FROM ITDE1.RUBA_DWH_DIM_TERMINALS_HIST t
LEFT JOIN ITDE1.RUBA_STG_DEL s
ON t.TERMINAL_ID = s.ID
	--AND EFFECTIVE_TO = TO_DATE('2999.12.31', 'YYYY.MM.DD')
	AND DELETED_FLG = 'N'
WHERE s.ID IS NULL
AND EFFECTIVE_TO = TO_DATE('2999.12.31', 'YYYY.MM.DD')
""")
curs.execute("""
UPDATE ITDE1.RUBA_DWH_DIM_TERMINALS_HIST 
SET EFFECTIVE_TO = (
		SELECT last_update 
    	FROM ITDE1.RUBA_META_LOADING
		WHERE dbname = 'ITDE1' 
		AND tablename = 'RUBA_DWH_DIM_TERMINALS_HIST' 
	) - INTERVAL '1' second
WHERE TERMINAL_ID IN (
	SELECT t.TERMINAL_ID
	FROM ITDE1.RUBA_DWH_DIM_TERMINALS_HIST t
	LEFT JOIN ITDE1.RUBA_STG_DEL s
	ON t.TERMINAL_ID = s.ID
		--AND  effective_to = to_date( '2999-12-31', 'YYYY-MM-DD' )
		AND  deleted_flg = 'N'
	WHERE s.id IS NULL 
	AND  effective_to = to_date( '2999-12-31', 'YYYY-MM-DD' )
	)
AND  effective_to = to_date( '2999-12-31', 'YYYY-MM-DD' )
AND  effective_from < (
		SELECT last_update 
    	FROM ITDE1.RUBA_META_LOADING
		WHERE dbname = 'ITDE1' 
		AND tablename = 'RUBA_DWH_DIM_TERMINALS_HIST' 
	)
""")

#--------------------------------------------------------
#--   ACCOUNTS
#--------------------------------------------------------
curs.execute( 'TRUNCATE TABLE ITDE1.RUBA_STG_DEL' )
curs.execute("""
INSERT INTO ITDE1.RUBA_STG_DEL ( ID )
SELECT ACCOUNT FROM BANK.ACCOUNTS
""")
curs.execute("""
INSERT INTO ITDE1.RUBA_DWH_DIM_ACCOUNTS_HIST( ACCOUNT_NUM, VALID_TO, CLIENT, EFFECTIVE_FROM, EFFECTIVE_TO, DELETED_FLG )
SELECT 
	t.ACCOUNT_NUM, 
	t.VALID_TO, 
	t.CLIENT, 
	SYSDATE,
	TO_DATE('2999.12.31', 'YYYY.MM.DD'),
	'Y'
FROM ITDE1.RUBA_DWH_DIM_ACCOUNTS_HIST t
LEFT JOIN ITDE1.RUBA_STG_DEL s
ON t.ACCOUNT_NUM = s.ID
	AND EFFECTIVE_TO = TO_DATE('2999.12.31', 'YYYY.MM.DD')
	AND DELETED_FLG = 'N'
WHERE s.ID IS NULL
""")
curs.execute("""
UPDATE ITDE1.RUBA_DWH_DIM_ACCOUNTS_HIST 
SET EFFECTIVE_TO = SYSDATE - INTERVAL '1' second
WHERE ACCOUNT_NUM IN (
	SELECT t.ACCOUNT_NUM
	FROM ITDE1.RUBA_DWH_DIM_ACCOUNTS_HIST  t
	LEFT JOIN ITDE1.RUBA_STG_DEL s
	ON t.ACCOUNT_NUM = s.ID
		AND  effective_to = to_date( '2999-12-31', 'YYYY-MM-DD' )
		AND  deleted_flg = 'N'
	WHERE s.id IS NULL )
AND  effective_to = to_date( '2999-12-31', 'YYYY-MM-DD' )
AND  effective_from < SYSDATE
""")

#--------------------------------------------------------
#--   CARDS
#--------------------------------------------------------
curs.execute( 'TRUNCATE TABLE ITDE1.RUBA_STG_DEL' )
curs.execute("""
INSERT INTO ITDE1.RUBA_STG_DEL ( ID )
SELECT REPLACE(CARD_NUM, ' ') FROM BANK.CARDS
""")
curs.execute("""
INSERT INTO ITDE1.RUBA_DWH_DIM_CARDS_HIST ( CARD_NUM, ACCOUNT_NUM, EFFECTIVE_FROM, EFFECTIVE_TO, DELETED_FLG )
SELECT 
	t.CARD_NUM, 
	t.ACCOUNT_NUM,  
	SYSDATE,
	TO_DATE('2999.12.31', 'YYYY.MM.DD'),
	'Y'
FROM ITDE1.RUBA_DWH_DIM_CARDS_HIST t
LEFT JOIN ITDE1.RUBA_STG_DEL s
ON t.CARD_NUM = s.ID
	AND EFFECTIVE_TO = TO_DATE('2999.12.31', 'YYYY.MM.DD')
	AND DELETED_FLG = 'N'
WHERE s.ID IS NULL
""")
curs.execute("""
UPDATE ITDE1.RUBA_DWH_DIM_CARDS_HIST
SET EFFECTIVE_TO = SYSDATE - INTERVAL '1' second
WHERE CARD_NUM IN (
	SELECT t.CARD_NUM
	FROM ITDE1.RUBA_DWH_DIM_CARDS_HIST t
	LEFT JOIN ITDE1.RUBA_STG_DEL s
	ON t.CARD_NUM = s.ID
		AND  effective_to = to_date( '2999-12-31', 'YYYY-MM-DD' )
		AND  deleted_flg = 'N'
	WHERE s.id IS NULL )
AND  effective_to = to_date( '2999-12-31', 'YYYY-MM-DD' )
AND  effective_from < SYSDATE
""")
#--------------------------------------------------------
#--   CLIENTS
#--------------------------------------------------------
curs.execute( 'TRUNCATE TABLE ITDE1.RUBA_STG_DEL' )
curs.execute("""
INSERT INTO ITDE1.RUBA_STG_DEL ( ID )
SELECT CLIENT_ID FROM BANK.CLIENTS
""")
curs.execute("""
INSERT INTO ITDE1.RUBA_DWH_DIM_CLIENTS_HIST ( CLIENT_ID, LAST_NAME, FIRST_NAME, PATRONYMIC, DATE_OF_BIRTH,
 PASSPORT_NUM, PASSPORT_VALID_TO, PHONE, EFFECTIVE_FROM, EFFECTIVE_TO, DELETED_FLG )
SELECT
	t.CLIENT_ID, 
	t.LAST_NAME, 
	t.FIRST_NAME, 
	t.PATRONYMIC, 
	t.DATE_OF_BIRTH, 
	t.PASSPORT_NUM, 
	t.PASSPORT_VALID_TO, 
	t.PHONE,   
	SYSDATE,
	TO_DATE('2999.12.31', 'YYYY.MM.DD'),
	'Y'
FROM ITDE1.RUBA_DWH_DIM_CLIENTS_HIST t
LEFT JOIN ITDE1.RUBA_STG_DEL s
ON t.CLIENT_ID = s.ID
	AND EFFECTIVE_TO = TO_DATE('2999.12.31', 'YYYY.MM.DD')
	AND DELETED_FLG = 'N'
WHERE s.ID IS NULL
""")
curs.execute("""
UPDATE ITDE1.RUBA_DWH_DIM_CLIENTS_HIST
SET EFFECTIVE_TO = SYSDATE - INTERVAL '1' second
WHERE CLIENT_ID IN (
	SELECT t.CLIENT_ID
	FROM ITDE1.RUBA_DWH_DIM_CLIENTS_HIST t
	LEFT JOIN ITDE1.RUBA_STG_DEL s
	ON t.CLIENT_ID = s.ID
		AND  effective_to = to_date( '2999-12-31', 'YYYY-MM-DD' )
		AND  deleted_flg = 'N'
	WHERE s.id IS NULL )
AND  effective_to = to_date( '2999-12-31', 'YYYY-MM-DD' )
AND  effective_from < SYSDATE
""")

#-- 6. Обновляем метаданные - дату максимальной загрузуки
curs.execute("""
UPDATE ITDE1.RUBA_META_LOADING
SET last_update = ( SELECT MAX( COALESCE (update_dt, create_dt) ) FROM ITDE1.RUBA_STG_ACCOUNTS )
WHERE 1=1
	AND dbname = 'ITDE1' 
	AND tablename = 'RUBA_DWH_DIM_ACCOUNTS_HIST' 
	AND ( SELECT MAX( COALESCE (update_dt, create_dt) ) FROM ITDE1.RUBA_STG_ACCOUNTS ) IS NOT NULL
""")
curs.execute("""
UPDATE ITDE1.RUBA_META_LOADING
SET last_update = ( SELECT MAX( COALESCE (update_dt, create_dt) ) FROM ITDE1.RUBA_STG_CARDS )
WHERE 1=1
	AND dbname = 'ITDE1' 
	AND tablename = 'RUBA_DWH_DIM_CARDS_HIST' 
	AND ( SELECT MAX( COALESCE (update_dt, create_dt) ) FROM ITDE1.RUBA_STG_CARDS ) IS NOT NULL
""")
curs.execute("""
UPDATE ITDE1.RUBA_META_LOADING
SET last_update = ( SELECT MAX( COALESCE (update_dt, create_dt) ) FROM ITDE1.RUBA_STG_CLIENTS )
WHERE 1=1
	AND dbname = 'ITDE1' 
	AND tablename = 'RUBA_DWH_DIM_CLIENTS_HIST' 
	AND ( SELECT MAX( COALESCE (update_dt, create_dt) ) FROM ITDE1.RUBA_STG_CLIENTS ) IS NOT NULL
""")

# Находим фрод по признаку "1"

curs.execute("""
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
""")

# Находим фрод по признаку "2"

curs.execute("""
INSERT INTO ITDE1.RUBA_REP_FRAUD( EVENT_DT, PASSPORT, FIO, PHONE, EVENT_TYPE, REPORT_DT )
SELECT 
	EVENT_DT, 
	passport_num, 
    FIO,
    PHONE,
	'2', 
	SYSDATE
FROM (
SELECT max(trans_date) AS EVENT_DT, passport_num, FIO, phone, fr.valid_to
FROM ITDE1.RUBA_DWH_FACT_TRANSACTIONS tr
INNER JOIN (
    SELECT card.card_num, ac.account_num, ac.client_id, ac.passport_num, ac.phone, ac.FIO, ac.valid_to
    FROM ITDE1.RUBA_DWH_DIM_CARDS_HIST card
    INNER JOIN (
        SELECT DISTINCT  cli.passport_num, cli.client_id, cli.phone, acc.valid_to, acc.account_num
        , TRIM(LAST_NAME) || ' ' || TRIM(FIRST_NAME) || ' ' || TRIM(PATRONYMIC) AS FIO
        FROM ITDE1.RUBA_DWH_DIM_ACCOUNTS_HIST acc
        INNER JOIN ITDE1.RUBA_DWH_DIM_CLIENTS_HIST cli
        ON acc.client = cli.client_id
    ) ac
    ON card.account_num = ac.account_num
) fr
ON tr.card_num = fr.card_num
GROUP BY passport_num, FIO, phone, valid_to
HAVING max(trans_date) > fr.valid_to
)
""")

# Находим фрод по признаку "3"

curs.execute("""
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
""")

# Находим фрод по признаку "4"

curs.execute("""
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
""")