# MySQL Teast Task

## Техническое задание: Разработка финансовых отчетов и создание процедур для финансового сервиса управления платежами

Вы претендуете на позицию MySQL разработчика, и в рамках этого технического задания мы предлагаем вам продемонстрировать свои навыки работы с реляционными базами данных. Задание включает разработку финансовых отчетов, создание хранимых процедур для обработки транзакций и обеспечение целостности данных.

Основная цель задания — оценить вашу способность эффективно проектировать запросы и оптимизировать их.

### Стукрутра таблиц:

*Операции пользователей*

Содержит в себе список денежных операций пользователей финансовго сервиса

```
CREATE TABLE payment.operations (
	id_operation BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
	dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
id_user INT UNSIGNED NOT NULL,
	id_type_oper SMALLINT UNSIGNED NOT NULL,
move TINYINT NOT NULL COMMENT 'направление движения (-1: списание со счёта, 1: начисление на счёт)',
	amount_oper DECIMAL(19, 5) NOT NULL COMMENT 'Сумма операции в валюте пользователя',
	PRIMARY KEY (id_operation)
) ENGINE = INNODB;
```

*Справочник типов операций*

Содержит в себе список всевозможных типов операций пользователей.  Поле id_type_oper в таблице payment.operations
Наример:
- Депозит средств на счет (Deposit)
- Снятие средств со счета (Withdrawal)
- Возврат средств (Refund)
- Штрафное списание (Penalty Charge)


```
CREATE TABLE payment.type_opers (
	id_type_oper SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
	name_oper VARCHAR(255) DEFAULT NULL COMMENT 'Название операции',
	comission DECIMAL(5, 2) DEFAULT 0.00 COMMENT 'Процент комиссии за операцию',
	PRIMARY KEY (id_type_oper)
) ENGINE = INNODB;
```

<table>
	<tr>
		<td valign="center" width="49%"><img src="https://github.com/Ruslan-Shevyrev/Ruslan-Shevyrev/blob/main/logoRS/logo_mini.gif" title="logo"></td>
		<td valign="center" width="49%"><img src="https://github.com/Ruslan-Shevyrev/Ruslan-Shevyrev/blob/main/logoRS/logoRS_FULL.png" title="RuslanShevyrev"></td>
	</tr>
</table>