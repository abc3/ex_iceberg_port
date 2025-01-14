.PHONY: bench

help:
	@make -qpRr | egrep -e '^[a-z].*:$$' | sed -e 's~:~~g' | sort

java_install_deps:
	cd java && mvn clean install

java_compile:
	cd java && mvn compile

dev:
	ERL_AFLAGS="-kernel shell_history enabled" iex --name node@127.0.0.1 --cookie cookie -S mix

bench:
	mix run bench/echo.exs