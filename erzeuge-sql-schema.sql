create schema finance;

create table finance.symbols (
	symbol varchar(4) primary key,
	name text,
	country text
);

create table finance.earnings_quarter (
	symbol varchar(4),
	quarter char(6),
	revenue bigint,
	earnings bigint,
	primary key (symbol, quarter)
);

-- Fremdschlüssel
ALTER TABLE finance.earnings_quarter ADD CONSTRAINT earnings_quarter_fk 
	FOREIGN KEY (symbol) REFERENCES finance.symbols(symbol);

create table finance.history (
	symbol varchar(4),
	"date" date,
	"open" numeric(15, 6),
	high numeric(15, 6),
	low numeric(15, 6),
	"close" numeric(15, 6),
	volume bigint,
	primary key(symbol, "date")
);

ALTER TABLE finance.history ADD CONSTRAINT history_fk 
	FOREIGN KEY (symbol) REFERENCES finance.symbols(symbol);



