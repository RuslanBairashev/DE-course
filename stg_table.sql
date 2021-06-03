--------------------------------------------------------
--  DDL for Table ACCOUNTS
--------------------------------------------------------

CREATE TABLE ITDE1.RUBA_STG_ACCOUNTS
(	ACCOUNT_NUM CHAR( 20 BYTE ), 
	VALID_TO DATE, 
	CLIENT VARCHAR2( 20 BYTE ), 
	CREATE_DT DATE, 
	UPDATE_DT DATE
);

--------------------------------------------------------
--  DDL for Table PAYMENT_CARDS
--------------------------------------------------------

CREATE TABLE ITDE1.RUBA_STG_CARDS
(	CARD_NUM VARCHAR2( 30 BYTE ), --
	ACCOUNT CHAR( 20 BYTE ), 
	CREATE_DT DATE, 
	UPDATE_DT DATE
);

--------------------------------------------------------
--  DDL for Table CLIENTS
--------------------------------------------------------

CREATE TABLE ITDE1.RUBA_STG_CLIENTS
(	CLIENT_ID VARCHAR2( 20 BYTE ), 
	LAST_NAME VARCHAR2( 100 BYTE ), 
	FIRST_NAME VARCHAR2( 100 BYTE ), 
	PATRONYMIC VARCHAR2( 100 BYTE ),
	DATE_OF_BIRTH DATE, 
	PASSPORT_NUM VARCHAR2( 15 BYTE ),
	PASSPORT_VALID_TO DATE, 
	PHONE VARCHAR2( 20 BYTE ),
	CREATE_DT DATE, 
	UPDATE_DT DATE
);

--------------------------------------------------------
--  DDL for Table BLACKLIST
--------------------------------------------------------

CREATE TABLE ITDE1.RUBA_STG_BLACKLIST
(	REPORT_DT VARCHAR2( 21 BYTE ), 
	PASSPORT_NUM VARCHAR2( 15 BYTE )
);

--------------------------------------------------------
--  DDL for Table TERMINALS
--------------------------------------------------------

CREATE TABLE ITDE1.RUBA_STG_TERMINALS
(	TERMINAL_ID VARCHAR2( 7 BYTE ), 
	TERMINAL_TYPE VARCHAR2( 5 BYTE ), 
	TERMINAL_CITY VARCHAR2( 50 BYTE ), 
	TERMINAL_ADDRESS VARCHAR2( 100 BYTE )
	);

--------------------------------------------------------
--  DDL for Table TRANSACTIONS
--------------------------------------------------------

CREATE TABLE ITDE1.RUBA_STG_TRANSACTIONS
(	TRANS_ID VARCHAR2( 15 BYTE ), 
	TRANS_DATE VARCHAR2( 21 BYTE ),
	AMT VARCHAR2( 30 BYTE ),
	CARD_NUM VARCHAR2( 30 BYTE ), 
	OPER_TYPE VARCHAR2( 10 BYTE ), 
	OPER_RESULT VARCHAR2( 10 BYTE ),
	TERMINAL VARCHAR2( 7 BYTE )
);

--------------------------------------------------------
--  DDL for Table DEL
--------------------------------------------------------

CREATE TABLE ITDE1.RUBA_STG_DEL
(	ID VARCHAR2( 30 BYTE )
);

--------------------------------------------------------
--  DDL for Table TERMINALS_RAW
--------------------------------------------------------

CREATE TABLE ITDE1.RUBA_STG_TERMINALS_RAW
(	TERMINAL_ID VARCHAR2( 7 BYTE ), 
	TERMINAL_TYPE VARCHAR2( 5 BYTE ), 
	TERMINAL_CITY VARCHAR2( 50 BYTE ), 
	TERMINAL_ADDRESS VARCHAR2( 100 BYTE )
	);