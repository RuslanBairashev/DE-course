CREATE TABLE ITDE1.RUBA_META_LOADING
(	DBNAME VARCHAR2(30),
	TABLENAME VARCHAR2(30),
	LAST_UPDATE DATE  
);

INSERT INTO ITDE1.RUBA_META_LOADING( DBNAME, TABLENAME, LAST_UPDATE )
	VALUES ( 'ITDE1', 'RUBA_DWH_DIM_ACCOUNTS_HIST', to_date( '1899-01-01', 'YYYY-MM-DD' ) );
INSERT INTO ITDE1.RUBA_META_LOADING( DBNAME, TABLENAME, LAST_UPDATE )
	VALUES ( 'ITDE1', 'RUBA_DWH_DIM_CARDS_HIST', to_date( '1899-01-01', 'YYYY-MM-DD' ) );
INSERT INTO ITDE1.RUBA_META_LOADING( DBNAME, TABLENAME, LAST_UPDATE )
	VALUES ( 'ITDE1', 'RUBA_DWH_DIM_CLIENTS_HIST', to_date( '1899-01-01', 'YYYY-MM-DD' ) );
INSERT INTO ITDE1.RUBA_META_LOADING( DBNAME, TABLENAME, LAST_UPDATE )
	VALUES ( 'ITDE1', 'RUBA_DWH_DIM_TERMINALS_HIST', to_date( '1899-01-01', 'YYYY-MM-DD' ) );

COMMIT;