/*Индекс добавил так как использую колоку dt в between операциях. Что должно ускорить выполнение запросов.*/
ALTER TABLE payment.operations ADD INDEX operations_dt_ind USING BTREE (dt)

/*Внешние ключи добавил для проверки корректности данных*/
ALTER TABLE payment.operations ADD CONSTRAINT operations_type_opers_FK FOREIGN KEY (id_type_oper) REFERENCES payment.type_opers(id_type_oper)
ALTER TABLE payment.operations ADD CONSTRAINT operations_users_FK FOREIGN KEY (id_users) REFERENCES payment.users(id_users)
ALTER TABLE payment.users ADD CONSTRAINT users_countries_FK FOREIGN KEY (id_country) REFERENCES payment.countries(id_country)
ALTER TABLE payment.users ADD CONSTRAINT users_currencies_FK FOREIGN KEY (id_currency) REFERENCES payment.currencies(id_currency)

/*задание 1*/
CREATE PROCEDURE payment.operation_commit(IN nID_USER INT, 
											IN nSUM_AMOUNT INT, 
											IN vNAME_OPER varchar(255),
											OUT vREPORT JSON,
											OUT nUSER_SUM_BEFORE_OPER INT,
											OUT nUSER_SUM_AFTER_OPER INT)
operation_commit:
BEGIN

	DECLARE nMOVE INT;

	DECLARE nID_TYPE_OPER INT;
	
	DECLARE nUSER_SUM_AMOUNT INT;
	
	DECLARE RESULT_TYPE varchar(20);
	
	DECLARE RESULT_MESSAGE varchar(4000);
	
	SET vREPORT := concat('{"ID_USER": ',nID_USER, ' }');
	
	SELECT USER_BALANCE
		INTO nUSER_SUM_AMOUNT
		FROM payment.users
		where ID_USER = nID_USER;
	
	SET nUSER_SUM_BEFORE_OPER := nUSER_SUM_AMOUNT;
	SET nUSER_SUM_AFTER_OPER := nUSER_SUM_AMOUNT;

	/* Не очень понятно как определяется какая операция для списания, а какая для зачисления. 
	 Я бы хранил эти параметры в справочнике type_opers и получал бы селектом. Но в контексте задания, скорее всего нельзя изменять таблицы, если это не указано. 
	 Поэтому пришлось вот так изголиться. Не очень понятно что делать когда появится новый типо операции. */

	IF lower(vNAME_OPER) IN ('deposit', 'refund') THEN
		SET nMOVE := 1;
	ELSEIF lower(vNAME_OPER) IN ('withdrawal', 'penalty charge') THEN
		SET nMOVE := -1;
	ELSE
		SELECT JSON_INSERT(vREPORT, '$.RESULT_TYPE', 'error', '$.RESULT_MESSAGE', 'Unknown operation type') INTO vREPORT;
		leave operation_commit;
	END IF;
	
	SELECT id_type_oper
		INTO nID_TYPE_OPER
		FROM payment.type_opers
		where NAME_OPER = vNAME_OPER;
		/*Считаю что сумма уже приходит в валюте пользователя*/
	IF nMOVE = -1 THEN
		IF nUSER_SUM_AMOUNT < nSUM_AMOUNT THEN
			SELECT JSON_INSERT(vREPORT, '$.RESULT_TYPE', 'error', '$.RESULT_MESSAGE', 'User have not enough money') INTO vREPORT;
			leave operation_commit;
		END IF;
	
		UPDATE payment.users 
			SET user_balance = user_balance - nSUM_AMOUNT
			where ID_USER = nID_USER;
		
		SET nUSER_SUM_AFTER_OPER := nUSER_SUM_AFTER_OPER - nSUM_AMOUNT;
	ELSE
	
		UPDATE payment.users 
			SET user_balance = user_balance + nSUM_AMOUNT
			where ID_USER = nID_USER;

		SET nUSER_SUM_AFTER_OPER := nUSER_SUM_AFTER_OPER + nSUM_AMOUNT;
	END IF;
	
	INSERT INTO payment.operations
		(id_user, id_type_oper, move, amount_oper)
		VALUES(nID_USER, nID_TYPE_OPER, nMOVE, nSUM_AMOUNT);
	
	SELECT JSON_INSERT(vREPORT, '$.RESULT_TYPE', 'success', '$.SUM_AMOUNT', nSUM_AMOUNT) INTO vREPORT;

	INSERT INTO payment.log_users
		(idUser, idAction, Params)
		VALUES(nID_USER, nID_TYPE_OPER, vREPORT);

END

/*CALL operation_commit (1, 100, 'deposit', 
  @REPORT, @USER_SUM_BEFORE_OPER, @USER_SUM_AFTER_OPER);

select @REPORT, @USER_SUM_BEFORE_OPER, @USER_SUM_AFTER_OPER;*/

/*задание 2*/

/* Возможно "отчет" означает, что нужно создать временную таблицу и положить туда данные, но из задания было не очень понятно.*/

/* Пояснения относительно использования with. В Oracle я бы написал hint MATERIALIZED, что бы закэшировать в памяти результат подзапроса, так как после группировки количество строк будет максимум количество стран * количество операций - что приемлемо для кэша. Но насколько я понял MySQL всегда кэширует with. Соответственно дополнительных хинтов не нужно. Да и из похожих я нашел только hint SUBQUERY(MATERIALIZATION)), который судя по всему используется только для in вхождений. План запроса так же показал, что with вызывается только 1 раз.*/

CREATE PROCEDURE financial_report(IN FROM_D datetime, IN TO_D datetime)
BEGIN
with t as(
	SELECT country_name, oper_name, sum(amount) as amount, sum(amount_comiss) as amount_comiss, sum(amount_no_comiss) as amount_no_comiss, 1 as order_country, 0 as order_total
			FROM(SELECT c.name_country as country_name,
						t.name_oper as oper_name,
						round(sum(o.amount_oper) / cur.base_rate, 2) as amount,
						round(round(sum(o.amount_oper) / cur.base_rate, 2) * t.comission / 100, 2) as amount_comiss,
						round(sum(o.amount_oper) / cur.base_rate, 2) - round(round(sum(o.amount_oper) / cur.base_rate, 2) * t.comission / 100, 2) as amount_no_comiss
					FROM payment.operations o
					INNER JOIN payment.users u
						ON o.id_user = u.id_user
					INNER JOIN payment.currencies cur
						ON u.id_currency = cur.id_currency
					INNER JOIN payment.countries c 
						ON u.id_country = c.id_country 
					INNER JOIN payment.type_opers t 
						ON o.id_type_oper = t.id_type_oper 
					WHERE dt between current_timestamp() - INTERVAL 10 DAY and current_timestamp()
					GROUP BY c.name_country, t.name_oper, cur.base_rate, t.comission) c
				GROUP BY country_name, oper_name),
	s as (SELECT country_name, oper_name, amount, amount_comiss, amount_no_comiss, 1 as order_country, 0 as order_total
			FROM t
		UNION ALL
		SElECT country_name, 'TOTAL', sum(amount), sum(amount_comiss), sum(amount_no_comiss), 2 as order_country, 0 as order_total
			FROM t
			GROUP BY country_name
		UNION ALL 
		SElECT 'TOTAL', 'TOTAL', sum(amount), sum(amount_comiss), sum(amount_no_comiss), 2 as order_country, 1 as order_total
			FROM t)
	SELECT country_name, oper_name, amount, amount_comiss, amount_no_comiss
		FROM s
		order by order_total, country_name, order_country, oper_name;
END

/*call financial_report(current_timestamp() - INTERVAL 1 DAY,  current_timestamp())*/

/*задание 3*/

/*Я бы еще подумал над тем, чтобы разбить таблицу operations_consolidation на партиции по наиболее часто используемым интервалам времени, допустим по дням, месяцам или неделям по аналогии с 6 заданием. Но не знаю нужно ли это было для этого задания.*/

CREATE TABLE payment.operations_consolidation (
	country_name VARCHAR(50) NOT NULL,
	oper_name VARCHAR(255) NOT NULL,
	amount DECIMAL(19, 5) NOT NULL,
	amount_comiss DECIMAL(19, 5) NOT NULL,
	amount_no_comiss DECIMAL(19, 5) NOT NULL,
	dt TIMESTAMP NOT NULL,
	PRIMARY KEY (country_name,oper_name,dt)
);

ALTER TABLE payment.operations_consolidation ADD INDEX operations_consolidation_dt_ind USING BTREE (dt);

CREATE PROCEDURE p_operations_consolidation()
BEGIN
INSERT INTO payment.operations_consolidation(country_name, oper_name, amount, amount_comiss, amount_no_comiss, dt)
	SELECT country_name, oper_name, amount, amount_comiss, amount_no_comiss, dt
		FROM (SELECT country_name, oper_name, sum(amount) as amount, sum(amount_comiss) as amount_comiss, sum(amount_no_comiss) as amount_no_comiss, dt
			FROM (SELECT c.name_country as country_name,
				t.name_oper as oper_name,
				round(sum(o.amount_oper) / cur.base_rate, 2) as amount,
				round(round(sum(o.amount_oper) / cur.base_rate, 2) * t.comission / 100, 2) as amount_comiss,
				round(sum(o.amount_oper) / cur.base_rate, 2) - round(round(sum(o.amount_oper) / cur.base_rate, 2) * t.comission / 100, 2) as amount_no_comiss,
				date(round(dt)) as dt
			FROM payment.operations o
			INNER JOIN payment.users u
				ON o.id_user = u.id_user
			INNER JOIN payment.currencies cur
				ON u.id_currency = cur.id_currency
			INNER JOIN payment.countries c 
				ON u.id_country = c.id_country 
			INNER JOIN payment.type_opers t 
				ON o.id_type_oper = t.id_type_oper 
			WHERE dt between DATE(current_timestamp() - INTERVAL 1 DAY) and DATE(current_timestamp()) - INTERVAL 1 SECOND 
			GROUP BY c.name_country, t.name_oper, cur.base_rate, t.comission, date(round(dt))) c
		GROUP BY country_name, oper_name, dt) t
	ON DUPLICATE KEY UPDATE amount = t.amount,
							amount_comiss = t.amount_comiss,
							amount_no_comiss = t.amount_no_comiss;

END

CREATE EVENT e_operations_consolidation
ON SCHEDULE EVERY 1 DAY
STARTS '2025-01-30 03:00:00'
DO
  call p_operations_consolidation();

/*call p_operations_consolidation();*/

/*задание 4*/

CREATE PROCEDURE financial_report_cons(IN FROM_D datetime, IN TO_D datetime)
financial_report_cons:
BEGIN
	
	DECLARE FROM_FULL_D datetime;
	DECLARE TO_FULL_D datetime;
	DECLARE FROM_1_PERIOD_D datetime;
	DECLARE TO_1_PERIOD_D datetime;
	DECLARE FROM_2_PERIOD_D datetime;
	DECLARE TO_2_PERIOD_D datetime;
	
	/* Если дата начала больше даты окончания, то ничего не делаем*/
	IF FROM_D > TO_D THEN
		LEAVE financial_report_cons;
	END IF;
	
	/* Проверяем что нет полных дней */
	IF (DATE(ROUND(FROM_D)) = DATE(ROUND(TO_D))) THEN
		SET FROM_1_PERIOD_D := FROM_D;
		SET TO_1_PERIOD_D := TO_D;
	ELSE
	/* Если полные дни есть то заполняем их и остатки */
		IF (DATE(ROUND(FROM_D)) = FROM_D) THEN
			set FROM_FULL_D := FROM_D;
			set FROM_1_PERIOD_D := NULL;
			SET TO_1_PERIOD_D := NULL;
		ELSE
			set FROM_FULL_D := DATE(FROM_D + INTERVAL 1 DAY);
			set FROM_1_PERIOD_D := FROM_D;
			SET TO_1_PERIOD_D := DATE(FROM_D + INTERVAL 1 DAY) - INTERVAL 1 SECOND;
			IF TO_1_PERIOD_D > TO_D THEN
				SET TO_1_PERIOD_D := TO_D;
			END IF;
		END IF;
	
		IF (DATE(ROUND(TO_D)) = TO_D) THEN
			set TO_FULL_D := TO_D;
			set FROM_2_PERIOD_D := NULL;
			SET TO_2_PERIOD_D := NULL;
		ELSE
			set TO_FULL_D := DATE(TO_D) - INTERVAL 1 SECOND;
			IF TO_1_PERIOD_D <> TO_D OR TO_1_PERIOD_D IS NULL THEN
				set FROM_2_PERIOD_D := DATE(TO_D);
				SET TO_2_PERIOD_D := TO_D;
			END IF;
		END IF;
	END IF;
		
	/*Зануляем неполные периоды*/
	IF FROM_FULL_D > TO_FULL_D THEN
		set FROM_FULL_D := NULL;
		SET TO_FULL_D := NULL;
	END IF;
	
with t as 
(SELECT country_name, oper_name, sum(amount) as amount, sum(amount_comiss) as amount_comiss, sum(amount_no_comiss) as amount_no_comiss
			FROM (SELECT country_name, oper_name, amount, amount_comiss, amount_no_comiss
			FROM payment.operations_consolidation
			WHERE dt BETWEEN FROM_FULL_D AND TO_FULL_D
			UNION ALL
			SELECT c.name_country as country_name,
						t.name_oper as oper_name,
						round(sum(o.amount_oper) / cur.base_rate, 2) as amount,
						round(round(sum(o.amount_oper) / cur.base_rate, 2) * t.comission / 100, 2) as amount_comiss,
						round(sum(o.amount_oper) / cur.base_rate, 2) - round(round(sum(o.amount_oper) / cur.base_rate, 2) * t.comission / 100, 2) as amount_no_comiss
					FROM payment.operations o
					INNER JOIN payment.users u
						ON o.id_user = u.id_user
					INNER JOIN payment.currencies cur
						ON u.id_currency = cur.id_currency
					INNER JOIN payment.countries c 
						ON u.id_country = c.id_country 
					INNER JOIN payment.type_opers t 
						ON o.id_type_oper = t.id_type_oper 
					WHERE dt between FROM_1_PERIOD_D and TO_1_PERIOD_D 
						OR dt between FROM_2_PERIOD_D and TO_2_PERIOD_D
					GROUP BY c.name_country, t.name_oper, cur.base_rate, t.comission) c 
				GROUP BY country_name, oper_name),
	s as (SELECT country_name, oper_name, amount, amount_comiss, amount_no_comiss, 1 as order_country, 0 as order_total
			FROM t
		UNION ALL
		SELECT country_name, 'TOTAL', sum(amount), sum(amount_comiss), sum(amount_no_comiss), 2 as order_country, 0 as order_total
			FROM t
			GROUP BY country_name
		UNION ALL 
		SElECT 'TOTAL', 'TOTAL', sum(amount), sum(amount_comiss), sum(amount_no_comiss), 2 as order_country, 1 as order_total
			FROM t)
	SELECT country_name, oper_name, amount, amount_comiss, amount_no_comiss
		FROM s
		order by order_total, country_name, order_country, oper_name;
END

/*call financial_report_cons(STR_TO_DATE('2025-01-29 00:00:00','%Y-%m-%d %H:%i:%s'), STR_TO_DATE('2025-01-31 01:00:00','%Y-%m-%d %H:%i:%s'));*/

/*задание 5*/

CREATE PROCEDURE financial_report_by_user(IN FROM_D datetime, IN TO_D datetime, IN USER_ID int)
BEGIN
	with t as(
	SELECT t.name_oper as oper_name,
			round(sum(o.amount_oper) / cur.base_rate, 2) as amount,
			round(round(sum(o.amount_oper) / cur.base_rate, 2) * t.comission / 100, 2) as amount_comiss,
			round(sum(o.amount_oper) / cur.base_rate, 2) - round(round(sum(o.amount_oper) / cur.base_rate, 2) * t.comission / 100, 2) as amount_no_comiss
		FROM payment.operations o
		INNER JOIN payment.users u
			ON o.id_user = u.id_user
		INNER JOIN payment.currencies cur
			ON u.id_currency = cur.id_currency
		INNER JOIN payment.countries c 
			ON u.id_country = c.id_country 
		INNER JOIN payment.type_opers t 
			ON o.id_type_oper = t.id_type_oper 
		WHERE dt between FROM_D and TO_D
			AND u.id_user = USER_ID
		GROUP BY t.name_oper, cur.base_rate, t.comission),
	s as (SElECT oper_name, amount, amount_comiss, amount_no_comiss,0 as order_total
			FROM t
		UNION ALL 
		SElECT 'TOTAL', sum(amount), sum(amount_comiss), sum(amount_no_comiss),1 as order_total
			FROM t)
	SELECT oper_name, amount, amount_comiss, amount_no_comiss
		FROM s
	order by order_total, oper_name;
END

/*call financial_report_by_user(current_timestamp() - INTERVAL 1 DAY,  current_timestamp(), 1)*/

/*задание 6*/

CREATE PROCEDURE payment.create_partitions()
BEGIN
	
	DECLARE vdate varchar(20);
	DECLARE vpartition varchar(20);
	DECLARE done INT DEFAULT FALSE;
	
	DECLARE curs CURSOR FOR SELECT 
		DATE_FORMAT(dt, "%Y-%m-%d 00:00:00"),
		concat('p_',DATE_FORMAT(dt, "%Y%m%d"))
		FROM
			(SELECT str_to_date(DATE_FORMAT(dt + INTERVAL 1 DAY, "%Y-%m-%d 00:00:00"), "%Y-%m-%d 00:00:00") AS dt
				FROM log_users) t
		GROUP BY dt
		ORDER BY dt;

	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
	
	SET @script := 'ALTER TABLE log_users PARTITION BY RANGE ( UNIX_TIMESTAMP(dt) ) (';
	
	OPEN curs;
	
	read_loop: LOOP
		FETCH curs INTO vdate, vpartition ;
	    IF done THEN
	      LEAVE read_loop;
	    END IF;
		SET @script := concat(@script, 'PARTITION ', vpartition,' VALUES LESS THAN ( UNIX_TIMESTAMP("', vdate, '")),');
	  END LOOP;
	
	CLOSE curs;
	
	SET @script := trim(trailing ',' from @script);
	
	SET @script := concat(@script, ' );');
	
	PREPARE stmt FROM @script;
	EXECUTE stmt;
	
END

call create_partitions()

CREATE PROCEDURE payment.logs_partitions()
BEGIN
	
	DECLARE vdate varchar(20);
	DECLARE vpartition_add varchar(20);
	DECLARE vpartition_drop varchar(20);
	DECLARE done INT DEFAULT FALSE;
	DECLARE vTABLE_NAME varchar(40);
	DECLARE nCNT INT;
	DECLARE nCOUNTER INT;
	DECLARE nMAX INT;
	
	DECLARE drop_cursor CURSOR FOR SELECT 
		partition_name
		FROM INFORMATION_SCHEMA.PARTITIONS
	    WHERE TABLE_NAME = vTABLE_NAME
	    	AND str_to_date(substr(partition_name, instr(partition_name, '_') + 1),"%Y%m%d") < now() - INTERVAL 7 DAY;
	
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

	/*Можно сделать этот параметр входным для процедуры и получится универсальная*/
	SET vTABLE_NAME := 'log_users';
	SET nCOUNTER := 0;
	SET nMAX := 2;
	
	OPEN drop_cursor;
	
	read_loop: LOOP
		FETCH drop_cursor INTO vpartition_drop;
	    IF done THEN
	      LEAVE read_loop;
	    END IF;
		SET @script := concat('ALTER TABLE ', vTABLE_NAME ,' DROP PARTITION ', vpartition_drop);
		PREPARE stmt_drop FROM @script;
		EXECUTE stmt_drop;
	  END LOOP;
	
	CLOSE drop_cursor;
	
	WHILE nCOUNTER <= nMAX do
		SET vdate := DATE_FORMAT(now() + INTERVAL nCOUNTER DAY, "%Y-%m-%d 00:00:00");
		SET vpartition_add := concat('p_',DATE_FORMAT(now() + INTERVAL nCOUNTER DAY, "%Y%m%d"));
		
		SELECT count(*)
			INTO nCNT
		    FROM INFORMATION_SCHEMA.PARTITIONS
		    WHERE TABLE_NAME = vTABLE_NAME
				AND PARTITION_NAME = vpartition_add;
		
		IF nCNT = 0 THEN
		
			SET @script := concat('ALTER TABLE ', vTABLE_NAME,' ADD PARTITION (PARTITION ', vpartition_add,' VALUES LESS THAN (UNIX_TIMESTAMP( "',vdate,'" )))');
			
			PREPARE stmt_alter FROM @script;
			EXECUTE stmt_alter;
		END IF;
		
    	set nCOUNTER = nCOUNTER+1;
  	END WHILE;
	
END

CREATE EVENT e_logs_partitions
ON SCHEDULE EVERY 1 DAY
STARTS '2025-01-30 02:00:00'
DO
  call logs_partitions();
