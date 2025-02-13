# MySQL Teast Task

## Техническое задание: Разработка финансовых отчетов и создание процедур для финансового сервиса управления платежами

Вы претендуете на позицию MySQL разработчика, и в рамках этого технического задания мы предлагаем вам продемонстрировать свои навыки работы с реляционными базами данных. Задание включает разработку финансовых отчетов, создание хранимых процедур для обработки транзакций и обеспечение целостности данных.

Основная цель задания — оценить вашу способность эффективно проектировать запросы и оптимизировать их.

### Стукрутра таблиц:

***Операции пользователей***

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

***Справочник типов операций***

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

***Логи действий пользователя***

Содержит в себе список всевозможных действий пользователей финансового сервиса.  
Наример:
- Пополнение счета (idAction 1);
- Вывод со счета (idAction 2);
- Авторизация в ситеме (idAction 3);
- Просмотр истории операций (idAction 4);
(поле в таблице idAction)

```
CREATE TABLE payment.log_users (
	dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	idUser INT UNSIGNED NOT NULL,
	idAction INT UNSIGNED NOT NULL,
	Params JSON DEFAULT NULL
) ENGINE = INNODB;
```

***Таблица с пользователями финансового сервиса***


Содержит в себе список пользователей, с их свойствами, пользователи выполняют денежные операции на сервисе финансов.

```
CREATE TABLE payment.users (
	id_user INT UNSIGNED NOT NULL AUTO_INCREMENT,
user_balance DECIMAL(14, 2) DEFAULT 0.00 COMMENT ‘Текущий баланс в валюте игрока’',
	id_currency SMALLINT UNSIGNED NOT NULL,
	id_country SMALLINT UNSIGNED NOT NULL,
	PRIMARY KEY (id_user)
) ENGINE = INNODB;
```

***Справочник валют***

Содержит в себе список всех возможных валют с курсами к рублю. Используется как свойство пользователя.

```
CREATE TABLE payment.currencies (
	id_currency SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
	name_currency VARCHAR(255) DEFAULT NULL COMMENT 'ISO код валюты (EUR / USD / BTC / TON ...)',
	base_rate DECIMAL(15, 5) DEFAULT NULL COMMENT 'Курс к рублю',
	PRIMARY KEY (id_currency)
) ENGINE = INNODB;
```

*Примечание для “base_rate”: чтобы получить эквивалент валюты в рублях, достаточно разделить базовую валюту на “base_rate”. Пример: валюта - EUR, base_rate - 0,010119000. 1 EUR / 0,010119000 = 98,8240 RUB*


***Справочник стран***

Содержит в себе список всех возможных стран. Используется как свойство пользователя

```
CREATE TABLE payment.countries (
	id_country SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
	name_country VARCHAR(50) DEFAULT NULL,
	PRIMARY KEY (id_country)
) ENGINE = INNODB;
```

***Аннотация к таблицам и хранящимся в них данным:***

1. Архитектура БД финансового сервиса, над которым вам предстоит работать, обрабатывает большое количество транзакций в режиме реального времени.
2. Таблица payment.operations содержит от 5 до 10 миллионов строк в день и хранит в себе данные за несколько лет.
3. Таблица payment.users содержит записи о 7 миллионах пользователей финансового сервиса.
4. Таблица payment.log_users содержит от 5 до 10 миллионов строк в день и хранит в себе данные за 10 лет.
5. Таблицы payment.type_opers, payment.currencies, payment.currencies  - справочники с количеством записей до 500.

***В этой части технического задания нужно будет добавить или не добавлять индексы к таблицам, при условии, что:***
1.	Необходимость индексов определяется исполнителем, исходя из условий данного технического задания;
2.	Наличие или отсутствие индекса должно быть аргументирован исполнителем.
3.	Все индексы, если они необходимы, нужно добавить в текстовый файл с решением.



<table>
	<tr>
		<td valign="center" width="49%"><img src="https://github.com/Ruslan-Shevyrev/Ruslan-Shevyrev/blob/main/logoRS/logo_mini.gif" title="logo"></td>
		<td valign="center" width="49%"><img src="https://github.com/Ruslan-Shevyrev/Ruslan-Shevyrev/blob/main/logoRS/logoRS_FULL.png" title="RuslanShevyrev"></td>
	</tr>
</table>