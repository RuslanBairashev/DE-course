--------------------------------------------------------
--  DDL for Table ACCOUNTS
--------------------------------------------------------

CREATE TABLE ITDE1.RUBA_DWH_DIM_ACCOUNTS_HIST
(	ACCOUNT_NUM VARCHAR2( 25 BYTE ), --
	VALID_TO DATE, 
	CLIENT VARCHAR2( 10 BYTE ), 
	EFFECTIVE_FROM DATE, 
	EFFECTIVE_TO DATE,
	DELETED_FLG CHAR( 1 BYTE )
);

--------------------------------------------------------
--  DDL for Table CARDS
--------------------------------------------------------

CREATE TABLE ITDE1.RUBA_DWH_DIM_CARDS_HIST
(	CARD_NUM VARCHAR2( 30 BYTE ), 
	ACCOUNT_NUM VARCHAR2( 25 BYTE ), 
	EFFECTIVE_FROM DATE, 
	EFFECTIVE_TO DATE,
	DELETED_FLG CHAR( 1 BYTE )
);

--------------------------------------------------------
--  DDL for Table CLIENTS
--------------------------------------------------------

CREATE TABLE ITDE1.RUBA_DWH_DIM_CLIENTS_HIST
(	CLIENT_ID VARCHAR2( 10 BYTE ), 
	LAST_NAME VARCHAR2( 100 BYTE ), 
	FIRST_NAME VARCHAR2( 100 BYTE ), 
	PATRONYMIC VARCHAR2( 100 BYTE ),
	DATE_OF_BIRTH DATE, 
	PASSPORT_NUM VARCHAR2( 15 BYTE ),
	PASSPORT_VALID_TO DATE, 
	PHONE VARCHAR2( 20 BYTE ),
	EFFECTIVE_FROM DATE, 
	EFFECTIVE_TO DATE,
	DELETED_FLG CHAR( 1 BYTE )
);

--------------------------------------------------------
--  DDL for Table BLACKLIST
--------------------------------------------------------

CREATE TABLE ITDE1.RUBA_DWH_FACT_BLACKLIST
(	PASSPORT_NUM VARCHAR2( 15 BYTE ),
	ENTRY_DT DATE
);

--------------------------------------------------------
--  DDL for Table TERMINALS
--------------------------------------------------------

CREATE TABLE ITDE1.RUBA_DWH_DIM_TERMINALS_HIST
(	TERMINAL_ID VARCHAR2( 7 BYTE ), 
	TERMINAL_TYPE VARCHAR2( 5 BYTE ), 
	TERMINAL_CITY VARCHAR2( 50 BYTE ), 
	TERMINAL_ADDRESS VARCHAR2( 100 BYTE ),
	EFFECTIVE_FROM DATE, 
	EFFECTIVE_TO DATE,
	DELETED_FLG CHAR( 1 BYTE )
);

--------------------------------------------------------
--  DDL for Table TRANSACTIONS
--------------------------------------------------------

CREATE TABLE ITDE1.RUBA_DWH_FACT_TRANSACTIONS
(	TRANS_ID VARCHAR2( 15 BYTE ), 
	TRANS_DATE DATE,
	CARD_NUM VARCHAR2( 30 BYTE ),
	OPER_TYPE VARCHAR2( 10 BYTE ), 
	AMT DECIMAL(30,2),
	OPER_RESULT VARCHAR2( 10 BYTE ),
	TERMINAL VARCHAR2( 7 BYTE )
);

--------------------------------------------------------
--  DDL for Table FRAUD
--------------------------------------------------------

CREATE TABLE ITDE1.RUBA_REP_FRAUD
(	EVENT_DT DATE,
	PASSPORT VARCHAR2( 15 BYTE ), 
	FIO VARCHAR2( 302 BYTE ), 
	PHONE VARCHAR2( 20 BYTE ), 
	EVENT_TYPE CHAR( 1 BYTE ), 
	REPORT_DT DATE
);
